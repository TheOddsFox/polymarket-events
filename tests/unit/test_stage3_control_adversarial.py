"""Prepared executions and receipts reject ambiguous or escaping control evidence."""

import hashlib
import json

import pytest

from fakes.harness import make_settings
from oddsfox_catalogue.capture.ledger import Ledger
from oddsfox_catalogue.certification import (
    BuildInvalid,
    assert_build_valid,
    prepare_dbt_execution,
    read_build_validity,
)
from oddsfox_catalogue.ids import iso_utc, utc_now


def test_escaping_artifact_target_is_rejected_before_dirtying_the_root(tmp_path):
    settings = make_settings(tmp_path / "operator")
    target = settings.state_dir / ".." / ".." / "outside" / "target"
    with pytest.raises(BuildInvalid):
        prepare_dbt_execution(settings, target)
    assert read_build_validity(settings) is None
    assert not settings.state_dir.exists()
    assert not (tmp_path / "outside").exists()


@pytest.mark.parametrize("revision", [True, 1.0])
def test_rechecksummed_receipt_requires_an_integer_schema_revision(tmp_path, monkeypatch, revision):
    settings = make_settings(tmp_path)
    binding = {"effective_vars": {}}
    receipt = {"revision": revision, "binding": binding}
    payload = json.dumps(receipt)
    with Ledger(settings.ledger_path) as ledger:
        ledger.set_build_validity(
            "valid",
            iso_utc(utc_now()),
            payload_json=payload,
            payload_sha256=hashlib.sha256(payload.encode()).hexdigest(),
        )
    monkeypatch.setattr("oddsfox_catalogue.certification._binding", lambda *_: binding)
    with pytest.raises(BuildInvalid, match="revision"):
        assert_build_valid(settings)
