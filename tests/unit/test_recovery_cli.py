"""Operator recovery failures are explicit and never overwrite an existing root."""

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

    arguments = ["backup", "restore", str(backup), "--destination", str(destination)]
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
