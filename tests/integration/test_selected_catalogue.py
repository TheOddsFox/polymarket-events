"""Selected and resumed acquisitions cannot silently expand or lose their scope."""

from __future__ import annotations

import json
import shutil
from datetime import timedelta

import duckdb
import httpx
import pytest

from fakes.dbt_run import run_dbt
from fakes.fake_gamma import FakeGamma, Rule
from fakes.harness import FIXED_NOW, FakeClock, make_settings
from fakes.world import World, event_stub, make_event, make_market
from oddsfox_catalogue import warehouse
from oddsfox_catalogue.capture import runner as capture_runner
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.capture.runner import CaptureRuntime, rebuild_from_raw, run_capture
from oddsfox_catalogue.capture.writer import read_body, read_manifest, verify_page, write_page
from oddsfox_catalogue.gamma.http import GammaClient, GammaError, RequestBudgetExceeded
from oddsfox_catalogue.load.runner import LoadRuntime, load_pending


def _world():
    world = World()
    for event_id, market_ids in (("11", ("201", "202")), ("12", ("203",))):
        event = make_event(event_id, "Selected event " + event_id)
        event["markets"] = [
            make_market(market_id, "Selected market " + market_id, event_stub=event_stub(event))
            for market_id in market_ids
        ]
        world.add_event(event)
    world.add_direct_market(make_market("204", "Direct only market"))
    return world


def _runtime(
    root, fake, *, now=FIXED_NOW, overrides=None, event_baseline=None, market_baseline=None
):
    settings = make_settings(root, overrides)
    clock = FakeClock()
    client = GammaClient(
        settings.gamma,
        transport=fake.transport(),
        clock=clock,
        sleep=clock.sleep,
        now=lambda: now,
        max_requests=settings.capture.max_requests,
        max_download_bytes=settings.capture.max_download_bytes,
        max_body_bytes=settings.capture.max_response_bytes,
        max_duration_s=settings.capture.max_duration_s,
    )
    rt = CaptureRuntime(
        settings=settings,
        client=client,
        ledger=Ledger(settings.ledger_path),
        now=lambda: now,
        open_event_ids=event_baseline,
        git_sha="synthetic",
    )
    rt.open_market_ids = market_baseline
    return rt


def _close(rt):
    rt.client.close()
    rt.ledger.close()


def _load(settings):
    with Ledger(settings.ledger_path) as ledger:
        return load_pending(LoadRuntime(settings=settings, ledger=ledger, now=lambda: FIXED_NOW))


def _rows(settings, sql):
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
        return con.execute(sql).fetchall()


def _observed_ids(settings, entity):
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='bronze' AND table_name=?",
            [entity + "_observations"],
        ).fetchone()[0]
        if not exists:
            return set()
        return {
            row[0]
            for row in con.execute(
                f"SELECT DISTINCT entity_id FROM bronze.{entity}_observations"
            ).fetchall()
        }


def _lookup(fake, entity, entity_id):
    return [
        (path, params)
        for path, params in fake.requests
        if path == f"/{entity}/{entity_id}"
        or (path.startswith(f"/{entity}") and entity_id in params.get("id", []))
    ]


def test_selected_market_keeps_parent_evidence_without_loading_incidental_siblings(tmp_path):
    world = _world()
    world.events["11"]["markets"][1]["events"] = [{"id": "12"}]
    world.markets["202"]["events"] = [{"id": "12"}]
    fake = FakeGamma(world)
    rt = _runtime(tmp_path, fake)
    try:
        summary = run_capture(rt, "selected", market_ids=["201"])
        assert summary.status == "captured"
        settings = rt.settings
        marker = next(settings.raw_dir.glob("*/*/_batch.json"))
        scope = json.loads(marker.read_text())["scope"]
        assert scope["market_ids"] == ["201"]
        assert scope["parent_event_ids"] == ["11"]
        assert scope["sealed"] is True
        assert _lookup(fake, "events", "12") == [], (
            "incidental sibling references cannot expand scope"
        )
    finally:
        _close(rt)
    raw = []
    for path in settings.raw_dir.glob("**/p*.manifest.json"):
        manifest = read_manifest(path)
        assert manifest is not None
        raw.append(read_body(path.parent, manifest))
    assert any(b'"202"' in body for body in raw), "retain complete parent response for replay"
    _load(settings)
    assert _observed_ids(settings, "market") == {"201"}
    assert _observed_ids(settings, "event") == {"11"}

    restored = tmp_path / "restored"
    replay_settings = make_settings(restored)
    shutil.copytree(settings.raw_dir, replay_settings.raw_dir)
    with Ledger(replay_settings.ledger_path) as ledger:
        rebuild_from_raw(replay_settings, ledger)
    _load(replay_settings)
    assert _observed_ids(replay_settings, "market") == {"201"}
    assert _observed_ids(replay_settings, "event") == {"11"}


def test_selected_event_only_does_not_nominate_embedded_market_assets(tmp_path):
    rt = _runtime(tmp_path, FakeGamma(_world()))
    try:
        assert run_capture(rt, "selected", event_ids=["11"]).status == "captured"
        settings = rt.settings
    finally:
        _close(rt)
    _load(settings)
    assert _observed_ids(settings, "event") == {"11"}
    assert _observed_ids(settings, "market") == set()


def test_combined_selection_unions_explicit_events_and_selected_market_parents_only(tmp_path):
    rt = _runtime(tmp_path, FakeGamma(_world()))
    try:
        captured = run_capture(rt, "selected", market_ids=["201"], event_ids=["12"])
        assert captured.status == "captured"
        settings = rt.settings
    finally:
        _close(rt)
    _load(settings)
    assert _observed_ids(settings, "event") == {"11", "12"}
    assert _observed_ids(settings, "market") == {"201"}


@pytest.mark.parametrize(
    ("market_ids", "event_ids"),
    [([], []), (["01"], []), (["../201"], []), ([], ["0"]), (["1"] * 101, []), ([], ["1"] * 101)],
)
def test_invalid_or_unbounded_selected_scope_is_rejected_before_http(
    tmp_path, market_ids, event_ids
):
    fake = FakeGamma(_world())
    rt = _runtime(tmp_path, fake)
    try:
        with pytest.raises(ValueError):
            run_capture(rt, "selected", market_ids=market_ids, event_ids=event_ids)
        assert fake.requests == []
        assert rt.client.stats.requests == 0
    finally:
        _close(rt)


def test_exactly_one_hundred_selected_ids_are_allowed_and_repeated_ids_do_not_expand_scope(
    tmp_path,
):
    world = World()
    for value in range(1, 101):
        world.add_direct_market(make_market(str(value), f"Market {value}"))
    rt = _runtime(tmp_path, FakeGamma(world))
    try:
        first = run_capture(rt, "selected", market_ids=[str(value) for value in range(1, 101)])
        assert first.status == "captured"
        second = run_capture(rt, "selected", market_ids=["1", "1", "1"])
        assert second.status == "captured"
        scope = json.loads(rt.ledger.get_batch(second.batch_id)["scope_json"])
        assert scope["market_ids"] == ["1"]
    finally:
        _close(rt)


def test_exactly_one_hundred_explicit_event_ids_are_allowed_without_market_expansion(tmp_path):
    world = World()
    for value in range(1, 101):
        world.add_event(make_event(str(value), f"Event {value}"))
    rt = _runtime(tmp_path, FakeGamma(world))
    try:
        captured = run_capture(rt, "selected", event_ids=[str(value) for value in range(1, 101)])
        assert captured.status == "captured"
        scope = json.loads(rt.ledger.get_batch(captured.batch_id)["scope_json"])
        assert len(scope["event_ids"]) == 100
        assert scope["market_ids"] == []
    finally:
        _close(rt)


def test_same_clock_fresh_acquisitions_never_reuse_batch_or_observation_ids(tmp_path):
    rt = _runtime(tmp_path, FakeGamma(_world()))
    try:
        first = run_capture(rt, "selected", market_ids=["201"])
        attempts = rt.client.stats.requests
        second = run_capture(rt, "selected", market_ids=["201"])
        assert first.batch_id != second.batch_id
        assert second.resumed is False
        assert rt.client.stats.requests > attempts
        assert len(rt.ledger.list_batches()) == 2
        settings = rt.settings
    finally:
        _close(rt)
    _load(settings)
    count, unique, batches = _rows(
        settings,
        "SELECT COUNT(*), COUNT(DISTINCT observation_id), COUNT(DISTINCT batch_id) "
        "FROM bronze.market_observations WHERE entity_id='201'",
    )[0]
    assert count == unique and count >= 2
    assert batches == 2


def test_unnamed_new_acquisition_does_not_implicitly_resume_interrupted_work(tmp_path):
    fake = FakeGamma(_world())
    fake.crash_on("/events/11")
    rt = _runtime(tmp_path, fake)
    try:
        with pytest.raises(SystemExit):
            run_capture(rt, "selected", market_ids=["201"])
        old = rt.ledger.list_batches()[0]["batch_id"]
        completed = run_capture(rt, "selected", market_ids=["201"])
        assert completed.batch_id != old and completed.resumed is False
        assert rt.ledger.get_batch(old)["status"] == "capturing"
        assert completed.status == "captured"
    finally:
        _close(rt)


def test_explicit_resume_of_verified_completed_batch_makes_no_http_requests(tmp_path):
    fake = FakeGamma(_world())
    rt = _runtime(tmp_path, fake)
    try:
        first = run_capture(rt, "selected", market_ids=["201"])
        fake.requests.clear()
        attempts = rt.client.stats.requests
        resumed = run_capture(rt, "selected", resume=first.batch_id)
        assert resumed.batch_id == first.batch_id and resumed.resumed is True
        assert resumed.status == "captured"
        assert fake.requests == []
        assert rt.client.stats.requests == attempts
    finally:
        _close(rt)


@pytest.mark.parametrize("tamper", ["gzip", "rehashed", "scope", "marker_symlink"])
def test_explicit_resume_reverifies_completed_evidence_before_http(tmp_path, tamper):
    fake = FakeGamma(_world())
    rt = _runtime(tmp_path, fake)
    try:
        first = run_capture(rt, "selected", market_ids=["201"])
        if tamper == "gzip":
            next(rt.settings.raw_dir.glob("**/*.json.gz")).write_bytes(b"corrupt")
        elif tamper == "rehashed":
            path = next(rt.settings.raw_dir.glob("**/p*.manifest.json"))
            manifest = read_manifest(path)
            assert manifest is not None
            body = read_body(path.parent, manifest) + b" "
            fields = {
                key: value
                for key, value in manifest.items()
                if key
                not in {"manifest_version", "seq", "file", "body_bytes", "body_sha256", "gz_sha256"}
            }
            write_page(path.parent, manifest["seq"], body, fields)
        elif tamper == "marker_symlink":
            marker = next(rt.settings.raw_dir.glob("*/*/_batch.json"))
            outside = tmp_path / "outside-batch.json"
            marker.rename(outside)
            marker.symlink_to(outside)
        else:
            marker = next(rt.settings.raw_dir.glob("*/*/_batch.json"))
            data = json.loads(marker.read_text())
            data["scope"]["market_ids"] = ["201", "202"]
            marker.write_text(json.dumps(data))
        fake.requests.clear()
        errors = (ValueError, RuntimeError)
        if tamper == "marker_symlink":
            errors += (RequestBudgetExceeded,)
        with pytest.raises(errors):
            run_capture(rt, "selected", resume=first.batch_id)
        assert fake.requests == []
    finally:
        _close(rt)


def test_named_resume_rejects_unknown_batch_mode_and_scope_mismatch_before_http(tmp_path):
    fake = FakeGamma(_world())
    rt = _runtime(tmp_path, fake)
    try:
        first = run_capture(rt, "selected", market_ids=["201"])
        fake.requests.clear()
        for mode, resume, ids in (
            ("selected", "missing-batch", []),
            ("daily", first.batch_id, []),
            ("selected", first.batch_id, ["202"]),
        ):
            with pytest.raises(ValueError):
                run_capture(rt, mode, resume=resume, market_ids=ids)
        assert fake.requests == []
    finally:
        _close(rt)


def test_budget_exhaustion_keeps_same_batch_resumable_with_new_invocation_allowance(tmp_path):
    fake = FakeGamma(_world())
    cap = {"CATALOGUE_CAPTURE_MAX_REQUESTS": "1"}
    rt = _runtime(tmp_path, fake, overrides=cap)
    try:
        with pytest.raises(RequestBudgetExceeded):
            run_capture(rt, "selected", market_ids=["201"])
        batch = rt.ledger.list_batches()[0]
        batch_id = batch["batch_id"]
        assert batch["status"] == "capturing"
        assert rt.client.stats.requests == 1
    finally:
        _close(rt)
    fake.requests.clear()
    rt = _runtime(tmp_path, fake, overrides=cap)
    try:
        completed = run_capture(rt, "selected", resume=batch_id)
        assert completed.status == "captured" and completed.batch_id == batch_id
        assert rt.client.stats.requests == 1
        assert completed.http_attempts == 1
        assert completed.downloaded_bytes == rt.client.stats.downloaded_bytes
        assert _lookup(fake, "markets", "201") == [], "verified completed unit must be skipped"
        assert _lookup(fake, "events", "11")
        stages = rt.ledger.stage_runs(batch_id)
        assert [row["status"] for row in stages] == ["failed", "succeeded"]
        measurements = [json.loads(row["counts_json"])["http"] for row in stages]
        assert [value["requests"] for value in measurements] == [1, 1]
        assert all(value["downloaded_bytes"] > 0 for value in measurements)
    finally:
        _close(rt)


def test_planning_http_attempts_share_the_capture_cap(tmp_path):
    fake = FakeGamma(_world())
    rt = _runtime(tmp_path, fake, overrides={"CATALOGUE_CAPTURE_MAX_REQUESTS": "1"})
    try:
        with pytest.raises(RequestBudgetExceeded):
            run_capture(rt, "bootstrap")
        assert rt.client.stats.requests == 1
        assert len(fake.requests) == 1
        assert not any(batch["status"] == "captured" for batch in rt.ledger.list_batches())
    finally:
        _close(rt)


def test_download_budget_exhaustion_never_commits_the_partial_unit_as_empty(tmp_path):
    fake = FakeGamma(_world())
    rt = _runtime(tmp_path, fake, overrides={"CATALOGUE_CAPTURE_MAX_DOWNLOAD_BYTES": "2"})
    try:
        with pytest.raises(RequestBudgetExceeded):
            run_capture(rt, "selected", market_ids=["201"])
        assert rt.client.stats.requests == 1
        assert rt.client.stats.downloaded_bytes > 2
        batches = rt.ledger.list_batches()
        assert len(batches) == 1 and batches[0]["status"] == "capturing"
        assert not list(rt.settings.raw_dir.glob("**/p*.manifest.json"))
    finally:
        _close(rt)


@pytest.mark.parametrize("quota", ["retained", "temporary"])
def test_existing_root_storage_above_quota_blocks_capture_before_http(tmp_path, quota):
    overrides = (
        {"CATALOGUE_CAPTURE_MAX_RETAINED_BYTES": "1"}
        if quota == "retained"
        else {"CATALOGUE_CAPTURE_MAX_TEMP_BYTES": "1"}
    )
    fake = FakeGamma(_world())
    rt = _runtime(tmp_path, fake, overrides=overrides)
    directory = rt.settings.data_dir if quota == "retained" else rt.settings.temporary_dir
    directory.mkdir(parents=True, exist_ok=True)
    retained = directory / "prior-evidence"
    retained.write_bytes(b"retain")
    try:
        with pytest.raises(RequestBudgetExceeded):
            run_capture(rt, "selected", market_ids=["201"])
        assert fake.requests == []
        assert retained.read_bytes() == b"retain"
    finally:
        _close(rt)


def test_resume_preserves_sealed_high_water_and_excludes_later_source_ids(tmp_path):
    world = _world()
    fake = FakeGamma(world)
    fake.crash_on("/markets/keyset")
    rt = _runtime(tmp_path, fake)
    try:
        with pytest.raises(SystemExit):
            run_capture(rt, "bootstrap")
        batch_id = rt.ledger.list_batches()[0]["batch_id"]
        world.add_event(make_event("99", "Appeared later"))
        world.add_direct_market(make_market("999", "Appeared later market"))
        completed = run_capture(rt, "bootstrap", resume=batch_id)
        assert completed.status == "captured"
        assert fake.calls_to("/events") == fake.calls_to("/markets") == 1
        scans = rt.ledger.list_scans(batch_id)
        assert not any("tail" in scan["scan_name"] for scan in scans)
        scope = json.loads(rt.ledger.get_batch(batch_id)["scope_json"])
        assert scope["high_water"] == {"events": 12, "markets": 204}
        assert scope["sealed"] is True
        settings = rt.settings
    finally:
        _close(rt)
    _load(settings)
    assert "99" not in _observed_ids(settings, "event")
    assert "999" not in _observed_ids(settings, "market")


def test_daily_refetches_direct_missing_open_unknown_records_and_404_preserves_prior_state(
    tmp_path,
):
    world = _world()
    world.markets["202"]["closed"] = None
    world.events["11"]["markets"][1]["closed"] = None
    fake = FakeGamma(world)
    rt = _runtime(tmp_path, fake)
    try:
        assert run_capture(rt, "bootstrap").status == "captured"
        settings = rt.settings
    finally:
        _close(rt)
    _load(settings)
    world.markets["201"]["closed"] = True
    world.markets["202"]["closed"] = True
    del world.markets["203"]
    world.events["12"]["markets"] = []
    fake.requests.clear()
    rt = _runtime(
        tmp_path,
        fake,
        now=FIXED_NOW + timedelta(days=1),
        event_baseline=lambda: {"11", "12"},
        market_baseline=lambda: {"201", "202", "203", "204"},
    )
    try:
        assert run_capture(rt, "daily").status == "captured"
        assert _lookup(fake, "markets", "201")
        assert _lookup(fake, "markets", "202")
        assert _lookup(fake, "markets", "203")
        assert _lookup(fake, "markets", "204") == []
        assert fake.calls_to("/markets/keyset") >= 1
    finally:
        _close(rt)
    _load(settings)
    built = run_dbt(["build"], settings.warehouse_path, tmp_path)
    assert built.returncode == 0, built.stdout[-5000:] + built.stderr[-3000:]
    assert _rows(
        settings,
        "SELECT market_id, closed FROM core.markets_current WHERE market_id IN ('201','202','203') ORDER BY market_id",
    ) == [("201", True), ("202", True), ("203", False)]


def test_daily_baseline_reads_include_unknown_lifecycle_records(tmp_path):
    path = tmp_path / "baseline.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA core")
        for entity in ("event", "market"):
            con.execute(
                f"CREATE TABLE core.{entity}s_current(venue VARCHAR,{entity}_id VARCHAR,closed BOOLEAN,archived BOOLEAN)"
            )
            con.execute(
                f"INSERT INTO core.{entity}s_current VALUES "
                "('polymarket','1',false,false),('polymarket','2',NULL,false),"
                "('polymarket','3',false,NULL),('polymarket','4',true,false),"
                "('polymarket','5',false,true),('other','6',false,false)"
            )
    assert warehouse.read_refresh_event_ids(path) == {"1", "2", "3"}
    assert warehouse.read_refresh_market_ids(path) == {"1", "2", "3"}


def test_daily_resume_uses_committed_baseline_instead_of_reading_changed_warehouse(tmp_path):
    world = _world()
    world.markets["201"]["closed"] = True
    fake = FakeGamma(world)
    fake.crash_on("/events/keyset")
    rt = _runtime(
        tmp_path,
        fake,
        event_baseline=lambda: {"11"},
        market_baseline=lambda: {"201"},
    )
    try:
        with pytest.raises(SystemExit):
            run_capture(rt, "daily")
        batch = rt.ledger.list_batches()[0]
        scope = json.loads(batch["scope_json"])
        assert scope["baseline"] == {"events": ["11"], "markets": ["201"]}
        assert scope["sealed"] is True
    finally:
        _close(rt)

    def changed_baseline():
        raise AssertionError("a named resume must use the committed daily baseline")

    fake.requests.clear()
    rt = _runtime(
        tmp_path,
        fake,
        now=FIXED_NOW + timedelta(days=1),
        event_baseline=changed_baseline,
        market_baseline=changed_baseline,
    )
    try:
        assert run_capture(rt, "daily", resume=batch["batch_id"]).status == "captured"
        assert _lookup(fake, "markets", "201")
        assert (
            json.loads(rt.ledger.get_batch(batch["batch_id"])["scope_json"])["baseline"]
            == scope["baseline"]
        )
    finally:
        _close(rt)


def _control_commit_crash(monkeypatch, phase, side="before"):
    """Interrupt the raw control commit, without forging ledger state in the test."""
    original = capture_runner.write_marker
    interrupted = []

    def write_marker(path, payload):
        scope = payload.get("scope", {})
        chosen = {
            "initial": payload.get("plan_stage") == -1 and not scope.get("high_water"),
            "high_water": payload.get("plan_stage") == -1
            and scope.get("high_water") == {"events": 12},
            "sealed": payload.get("plan_stage") == 0 and scope.get("sealed") is True,
            "parents": payload.get("plan_stage") == 2 and scope.get("parent_event_ids") == ["11"],
            "scan_complete": payload.get("status") == "complete",
            "finalized": payload.get("status") == "captured",
        }[phase]
        marker = "_scan.json" if phase == "scan_complete" else "_batch.json"
        if path.name == marker and chosen and not interrupted:
            if side == "after":
                original(path, payload)
            interrupted.append(payload)
            raise SystemExit("control manifest commit interrupted")
        return original(path, payload)

    monkeypatch.setattr(capture_runner, "write_marker", write_marker)
    return interrupted


@pytest.mark.parametrize(
    "phase", ["initial", "high_water", "sealed", "parents", "scan_complete", "finalized"]
)
@pytest.mark.parametrize("side", ["before", "after"])
def test_control_commit_interruption_is_recoverable_without_reusing_source_units(
    tmp_path, monkeypatch, phase, side
):
    fake = FakeGamma(_world())
    rt = _runtime(tmp_path, fake)
    mode = "bootstrap" if phase == "high_water" else "selected"
    with monkeypatch.context() as patch:
        interrupted = _control_commit_crash(patch, phase, side)
        try:
            with pytest.raises(SystemExit, match="control manifest"):
                run_capture(rt, mode, market_ids=[] if mode == "bootstrap" else ["201"])
            assert len(interrupted) == 1
            batch = rt.ledger.list_batches()[0]
            committed = {
                page["page_id"]: page["body_sha256"]
                for page in rt.ledger.pages_for_batch(batch["batch_id"])
            }
            settings = rt.settings
        finally:
            _close(rt)

    fake.requests.clear()
    rt = _runtime(tmp_path, fake)
    try:
        resumed = run_capture(rt, mode, resume=batch["batch_id"])
        assert resumed.status == "captured" and resumed.batch_id == batch["batch_id"]
        assert resumed.resumed is True
        assert rt.client.stats.requests <= settings.capture.max_requests
        after = {
            page["page_id"]: page["body_sha256"]
            for page in rt.ledger.pages_for_batch(batch["batch_id"])
        }
        assert committed.items() <= after.items(), "verified completed evidence cannot be replaced"
        if phase in {"parents", "scan_complete", "finalized"}:
            assert _lookup(fake, "markets", "201") == []
        if phase == "finalized":
            assert fake.requests == []
        if phase == "high_water":
            assert fake.calls_to("/events") == 0, "sealed HWM discovery is already committed"
    finally:
        _close(rt)

    replay_settings = make_settings(tmp_path / "reconstructed")
    shutil.copytree(settings.raw_dir, replay_settings.raw_dir)
    with Ledger(replay_settings.ledger_path) as ledger:
        rebuilt = rebuild_from_raw(replay_settings, ledger)
        assert rebuilt["batches"] == 1
        assert ledger.get_batch(batch["batch_id"])["status"] == "captured"
    _load(replay_settings)
    assert "201" in _observed_ids(replay_settings, "market")


@pytest.mark.parametrize("tamper", ["marker", "ledger"])
def test_control_recovery_does_not_accept_unrelated_scope_changes(tmp_path, monkeypatch, tamper):
    fake = FakeGamma(_world())
    rt = _runtime(tmp_path, fake)
    with monkeypatch.context() as patch:
        _control_commit_crash(patch, "parents")
        try:
            with pytest.raises(SystemExit, match="control manifest"):
                run_capture(rt, "selected", market_ids=["201"])
            batch = rt.ledger.list_batches()[0]
            if tamper == "ledger":
                scope = json.loads(batch["scope_json"])
                scope["parent_event_ids"] = ["99"]
                with rt.ledger.transaction() as con:
                    con.execute(
                        "UPDATE batches SET scope_json=? WHERE batch_id=?",
                        [json.dumps(scope), batch["batch_id"]],
                    )
            else:
                path = next(rt.settings.raw_dir.glob("*/*/_batch.json"))
                marker = json.loads(path.read_text())
                marker["scope"]["market_ids"] = ["201", "202"]
                path.write_text(json.dumps(marker))
        finally:
            _close(rt)

    fake.requests.clear()
    rt = _runtime(tmp_path, fake)
    try:
        with pytest.raises((ValueError, RuntimeError)):
            run_capture(rt, "selected", resume=batch["batch_id"])
        assert fake.requests == [], "repair must verify its recorded transition before source calls"
    finally:
        _close(rt)


@pytest.mark.parametrize("fault", ["after_control_ledger_commit", "after_control_marker_commit"])
def test_declared_control_fault_points_leave_a_verified_resumable_acquisition(
    tmp_path, monkeypatch, fault
):
    fake = FakeGamma(_world())
    rt = _runtime(tmp_path, fake)

    def interrupt(point):
        if point == fault:
            raise SystemExit(fault)

    with monkeypatch.context() as patch:
        patch.setattr(capture_runner, "fault_point", interrupt)
        try:
            with pytest.raises(SystemExit, match=fault):
                run_capture(rt, "selected", market_ids=["201"])
            batch = rt.ledger.list_batches()[0]
            assert batch["status"] == "capturing"
            assert fake.requests == [], "initial intent is committed before source discovery"
        finally:
            _close(rt)

    rt = _runtime(tmp_path, fake)
    try:
        captured = run_capture(rt, "selected", resume=batch["batch_id"])
        assert captured.status == "captured" and captured.batch_id == batch["batch_id"]
        assert captured.resumed is True
        assert run_capture(rt, "selected", resume=batch["batch_id"]).http_attempts == 0
    finally:
        _close(rt)


def test_pending_initial_control_cannot_be_relocated_within_the_raw_root(tmp_path, monkeypatch):
    fake = FakeGamma(_world())
    rt = _runtime(tmp_path, fake)
    with monkeypatch.context() as patch:
        _control_commit_crash(patch, "initial")
        try:
            with pytest.raises(SystemExit, match="control manifest"):
                run_capture(rt, "selected", market_ids=["201"])
            batch = rt.ledger.list_batches()[0]
            (control,) = rt.ledger.pending_controls(batch["batch_id"])
            unauthorized_id = "2026-10-08/unrelated-acquisition/_batch.json"
            unauthorized = rt.settings.raw_dir / unauthorized_id
            with rt.ledger.transaction() as con:
                con.execute(
                    "UPDATE capture_controls SET control_id=? WHERE control_id=?",
                    [unauthorized_id, control["control_id"]],
                )
        finally:
            _close(rt)

    rt = _runtime(tmp_path, fake)
    try:
        with pytest.raises((ValueError, RuntimeError)):
            run_capture(rt, "selected", resume=batch["batch_id"])
        assert fake.requests == []
        assert not unauthorized.exists(), "control locators must match the declared acquisition"
    finally:
        _close(rt)


@pytest.mark.parametrize("failure", ["retry_exhaustion", "wrong_identity", "invalid_envelope"])
def test_failed_selected_source_unit_is_resumable_and_never_committed_as_empty(tmp_path, failure):
    fake = FakeGamma(_world())
    if failure == "retry_exhaustion":
        fake.fail_status("/markets/201", 503, times=5)
    else:
        response = {"id": "202"} if failure == "wrong_identity" else {"unrelated": []}
        fake.rules.append(
            Rule(
                lambda request: request.url.path == "/markets/201",
                lambda _: httpx.Response(200, json=response),
            )
        )
    rt = _runtime(tmp_path, fake)
    try:
        with pytest.raises(GammaError):
            run_capture(rt, "selected", market_ids=["201"])
        batch = rt.ledger.list_batches()[0]
        assert batch["status"] == "capturing"
        assert rt.ledger.pages_for_batch(batch["batch_id"]) == []
        assert not list(rt.settings.raw_dir.glob("**/p*.manifest.json"))
        assert 0 < rt.client.stats.requests <= rt.settings.capture.max_requests
        assert rt.client.stats.downloaded_bytes > 0
    finally:
        _close(rt)

    fake.requests.clear()
    rt = _runtime(tmp_path, fake)
    try:
        resumed = run_capture(rt, "selected", resume=batch["batch_id"])
        assert resumed.status == "captured" and resumed.batch_id == batch["batch_id"]
        assert _lookup(fake, "markets", "201")
        pages = rt.ledger.pages_for_batch(batch["batch_id"])
        assert all(page["http_status"] == 200 for page in pages)
        assert sum(page["record_count"] for page in pages) >= 2
    finally:
        _close(rt)


@pytest.mark.parametrize("field", ["page_id", "batch_id", "scan_id", "record_key", "endpoint"])
def test_valid_orphan_payload_cannot_be_adopted_under_forged_capture_identity(
    tmp_path, monkeypatch, field
):
    fake = FakeGamma(_world())
    rt = _runtime(tmp_path, fake)

    def interrupt(point):
        if point == "after_page_rename":
            raise SystemExit("orphan page committed")

    with monkeypatch.context() as patch:
        patch.setattr(capture_runner, "fault_point", interrupt)
        try:
            with pytest.raises(SystemExit, match="orphan page"):
                run_capture(rt, "selected", market_ids=["201"])
            batch = rt.ledger.list_batches()[0]
            assert rt.ledger.pages_for_batch(batch["batch_id"]) == []
            path = next(rt.settings.raw_dir.glob("**/p*.manifest.json"))
            manifest = read_manifest(path)
            body = read_body(path.parent, manifest) + b" "
            fields = {
                key: value
                for key, value in manifest.items()
                if key
                not in {"manifest_version", "seq", "file", "body_bytes", "body_sha256", "gz_sha256"}
            }
            fields[field] = {
                "page_id": "foreign-page-id",
                "batch_id": "20261008T060000Z-selected-99",
                "scan_id": "foreign-scan-id",
                "record_key": "events",
                "endpoint": "/events/201",
            }[field]
            write_page(path.parent, manifest["seq"], body, fields)
            assert verify_page(path.parent, read_manifest(path)), "bytes and checksums are valid"
            retained_manifest = path.read_bytes()
            retained_payload = (path.parent / manifest["file"]).read_bytes()
        finally:
            _close(rt)

    fake.requests.clear()
    rt = _runtime(tmp_path, fake)
    try:
        with pytest.raises((ValueError, RuntimeError)):
            run_capture(rt, "selected", resume=batch["batch_id"])
        assert fake.requests == [], "orphan identity must be verified before more source requests"
        assert rt.ledger.pages_for_batch(batch["batch_id"]) == []
        assert path.read_bytes() == retained_manifest
        assert (path.parent / manifest["file"]).read_bytes() == retained_payload
    finally:
        _close(rt)


def test_daily_closure_of_known_market_above_reduced_high_water_is_loaded(tmp_path):
    world = World()
    world.add_direct_market(make_market("1", "Still listed"))
    world.add_direct_market(make_market("201", "Known prior open market"))
    fake = FakeGamma(world)
    rt = _runtime(tmp_path, fake)
    try:
        assert run_capture(rt, "bootstrap").status == "captured"
        settings = rt.settings
    finally:
        _close(rt)
    _load(settings)
    world.markets["201"]["closed"] = True
    fake.rules.append(
        Rule(
            lambda request: (
                request.url.path == "/markets"
                and request.url.params.get("order") == "id"
                and request.url.params.get("ascending") == "false"
            ),
            lambda _: httpx.Response(200, json=[{"id": "1"}]),
        )
    )
    fake.requests.clear()
    rt = _runtime(
        tmp_path,
        fake,
        now=FIXED_NOW + timedelta(days=1),
        event_baseline=lambda: set(),
        market_baseline=lambda: {"1", "201"},
    )
    try:
        captured = run_capture(rt, "daily")
        assert captured.status == "captured"
        scope = json.loads(rt.ledger.get_batch(captured.batch_id)["scope_json"])
        assert scope["high_water"]["markets"] == 1
        assert _lookup(fake, "markets", "201")
    finally:
        _close(rt)
    _load(settings)
    assert _rows(
        settings,
        "SELECT json_extract(normalized,'$.closed')::BOOLEAN FROM bronze.market_observations "
        "WHERE entity_id='201' AND observed_at > TIMESTAMPTZ '2026-10-08 06:00:00+00'",
    ) == [(True,)]
