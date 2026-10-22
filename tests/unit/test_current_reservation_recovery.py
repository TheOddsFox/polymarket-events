"""Recovery for saved publication estimates that consume an invocation."""

from contextlib import nullcontext
from dataclasses import replace

import duckdb
import pytest

from oddsfox_catalogue import current_runtime, current_store
from oddsfox_catalogue.cli import build_parser
from oddsfox_catalogue.current_config import CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import CurrentError


@pytest.fixture
def catalogue(tmp_path):
    settings = CurrentSettings(tmp_path / "catalogue")
    validate_root(settings.root, initialize=True)
    conn = duckdb.connect(str(settings.store_path))
    current_store.initialize(conn)
    yield settings, conn
    conn.close()


def test_publish_recalibrate_is_an_explicit_cli_option():
    args = build_parser().parse_args(["--root", "/tmp/catalogue", "publish", "--recalibrate"])
    assert args.command == "publish"
    assert args.recalibrate is True


def test_impossible_saved_reservation_blocks_before_client_or_pending_resume(
    catalogue, monkeypatch
):
    settings, conn = catalogue
    settings = replace(
        settings,
        limits=replace(settings.limits, max_duration_s=120),
    )
    # The dynamic reserve is 1.5 * the last measured publication duration.
    current_store.set_state(conn, "previous_publish_seconds", 80)
    monkeypatch.setattr(current_runtime, "connect_store", lambda _: nullcontext(conn))

    def unexpected_client(*args, **kwargs):
        pytest.fail("acquisition client must not be constructed")

    monkeypatch.setattr(current_runtime, "GammaClient", unexpected_client)
    from oddsfox_catalogue import current_sync

    monkeypatch.setattr(
        current_sync,
        "resume_pending",
        lambda *args, **kwargs: pytest.fail("pending work must not be resumed"),
    )

    with pytest.raises(CurrentError, match=r"120\.00s.*publish --recalibrate"):
        current_runtime.execute(
            settings,
            "refresh",
            {},
            deadline=120,
        )

    assert current_store.get_state(conn, "previous_publish_seconds") == 80


def test_reservation_equal_to_whole_invocation_limit_is_blocked(catalogue, monkeypatch):
    settings, conn = catalogue
    settings = replace(
        settings,
        limits=replace(settings.limits, max_duration_s=120),
    )
    current_store.set_state(conn, "previous_publish_seconds", 80)
    monkeypatch.setattr(current_runtime, "connect_store", lambda _: nullcontext(conn))
    monkeypatch.setattr(
        current_runtime,
        "GammaClient",
        lambda *args, **kwargs: pytest.fail("equality must block acquisition"),
    )

    with pytest.raises(CurrentError, match="consumes the configured invocation limit"):
        current_runtime.execute(settings, "reconcile", {"full_pass": False}, deadline=120)
