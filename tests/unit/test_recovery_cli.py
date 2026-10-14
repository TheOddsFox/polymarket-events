"""Operator recovery failures are explicit and never overwrite an existing root."""

import json

import pytest

from oddsfox_catalogue.backup import create_backup
from oddsfox_catalogue.cli import main
from oddsfox_catalogue.config import Settings


def test_restore_cli_copies_verified_evidence_and_refuses_existing_destination(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "source"
    evidence = source / "data" / "metadata" / "evidence.json"
    evidence.parent.mkdir(parents=True)
    evidence.write_text('{"synthetic": true}\n')
    backup = create_backup(Settings(source), dest_root=tmp_path / "backups")
    destination = tmp_path / "restored"
    monkeypatch.setenv("CATALOGUE_ROOT", str(source))

    arguments = [
        "backup",
        "restore",
        str(backup),
        "--destination",
        str(destination),
        "--max-retained-bytes",
        "65536",
    ]
    assert main(arguments) == 0
    assert (
        destination / "data" / "metadata" / "evidence.json"
    ).read_bytes() == evidence.read_bytes()
    capsys.readouterr()

    marker = destination / "operator-note"
    marker.write_text("retain")
    assert main(arguments) == 3
    assert marker.read_text() == "retain"
    assert "fresh destination" in capsys.readouterr().err


def test_verify_cli_without_a_release_fails_explicitly(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path))
    assert main(["verify"]) == 3
    assert "no published release" in capsys.readouterr().err
    assert not (tmp_path / "data" / "published").exists()


def test_backup_create_final_verification_uses_the_explicit_operator_cap(
    tmp_path, monkeypatch, capsys
):
    import oddsfox_catalogue.cli as cli

    original = cli.verify_backup
    observed = []

    def verify(path, *, max_bytes):
        observed.append(max_bytes)
        return original(path, max_bytes=max_bytes)

    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path / "source"))
    cap = 96 * 1024**3
    monkeypatch.setenv("CATALOGUE_CAPTURE_MAX_RETAINED_BYTES", str(cap))
    monkeypatch.setattr(cli, "verify_backup", verify)
    assert cli.main(["backup", "create", "--dest", str(tmp_path / "backups")]) == 0
    assert observed == [cap]
    capsys.readouterr()


@pytest.mark.parametrize("explicit_release", [False, True])
def test_verify_checks_release_once_through_the_correct_pointer_or_explicit_path(
    tmp_path, monkeypatch, capsys, explicit_release
):
    from oddsfox_catalogue import cli, publish

    settings = Settings(tmp_path)
    release_id = "20261014T101112Z"
    created_at = "2026-10-14T10:11:12Z"
    release = settings.published_dir / "releases" / release_id
    release.mkdir(parents=True)
    manifest_sha256 = "a" * 64
    (settings.published_dir / publish.CURRENT_POINTER).write_text(
        json.dumps(
            {
                "release_id": release_id,
                "path": f"releases/{release_id}",
                "published_at": created_at,
                "manifest_sha256": manifest_sha256,
            }
        )
    )
    verified, direct_calls = [], []

    def verify(selected, path, expected_hash=None):
        assert selected.root == settings.root
        verified.append((path, expected_hash))
        return {"release_id": release_id, "created_at": created_at}

    def direct_verify(*args, **kwargs):
        direct_calls.append(True)
        return verify(*args, **kwargs)

    monkeypatch.setenv("CATALOGUE_ROOT", str(tmp_path))
    monkeypatch.setattr(publish, "verify_release", verify)
    monkeypatch.setattr(cli, "verify_release", direct_verify)
    assert cli.current_release is publish.current_release
    arguments = ["verify", *(["--release", str(release)] if explicit_release else [])]
    assert cli.main(arguments) == 0
    assert verified == [(release, None if explicit_release else manifest_sha256)]
    assert direct_calls == ([True] if explicit_release else [])
    assert json.loads(capsys.readouterr().out) == {
        "verified": True,
        "release_id": release_id,
        "path": str(release),
    }
