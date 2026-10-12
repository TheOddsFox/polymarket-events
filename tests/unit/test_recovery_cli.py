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
