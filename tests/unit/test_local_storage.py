from pathlib import Path

import pytest

from oddsfox_catalogue import cli, current_config, metadata
from oddsfox_catalogue.current_config import (
    CurrentError,
    CurrentSettings,
    data_path,
    load_current_settings,
    repository_data_dir,
    validate_root,
)


def existing_external_catalogue(tmp_path, monkeypatch):
    data_anchor = tmp_path / "policy-data"
    data_anchor.mkdir()
    monkeypatch.setattr(current_config, "repository_data_dir", lambda: data_anchor)
    root = tmp_path / "external-catalogue"
    root.mkdir()
    (root / "catalogue.duckdb").write_bytes(b"existing fake store")
    return data_anchor, CurrentSettings(root)


def test_checkout_default_is_independent_of_cwd_and_explicit_root_wins(tmp_path, monkeypatch):
    data_dir = repository_data_dir()
    assert data_dir is not None
    assert data_path(tmp_path).is_relative_to(data_dir)
    monkeypatch.chdir(tmp_path)

    assert load_current_settings(env={}).root == data_dir / "catalogue-v3"
    explicit = tmp_path / "explicit"
    assert (
        load_current_settings(explicit, env={"CATALOGUE_ROOT": str(tmp_path / "environment")}).root
        == explicit
    )


def test_checkout_is_detected_for_package_installed_under_runtime_data(monkeypatch):
    data_dir = repository_data_dir()
    assert data_dir is not None
    installed_origin = (
        data_dir / "catalogue-runtime" / "site-packages" / "oddsfox_catalogue" / "current_config.py"
    )
    monkeypatch.setattr(current_config, "__file__", str(installed_origin))

    assert current_config.repository_data_dir() == data_dir


def test_checkout_detection_fails_closed_on_unreadable_project_metadata(monkeypatch):
    data_dir = repository_data_dir()
    assert data_dir is not None
    project_file = data_dir.parent / "pyproject.toml"
    read_text = Path.read_text

    def unreadable(path, *args, **kwargs):
        if path == project_file:
            raise PermissionError("synthetic unreadable project metadata")
        return read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", unreadable)
    with pytest.raises(CurrentError, match="cannot identify the local catalogue checkout"):
        current_config.repository_data_dir()


def test_checkout_detection_rejects_renamed_project_before_root_creation(tmp_path, monkeypatch):
    checkout = tmp_path / "renamed-checkout"
    source_file = checkout / "src" / "oddsfox_catalogue" / "current_config.py"
    source_file.parent.mkdir(parents=True)
    source_file.write_text("")
    (checkout / "pyproject.toml").write_text('[project]\nname = "renamed-project"\n')
    root = checkout / "data" / "catalogue-v3"
    installed_origin = (
        checkout
        / "data"
        / "catalogue-runtime"
        / "site-packages"
        / "oddsfox_catalogue"
        / "current_config.py"
    )
    monkeypatch.setattr(current_config, "__file__", str(installed_origin))

    with pytest.raises(CurrentError, match="unexpected local catalogue project identity"):
        validate_root(root, initialize=True)
    assert not root.exists()


def test_standalone_package_keeps_cwd_default_and_safe_path_behavior(tmp_path, monkeypatch):
    outside_origin = (
        repository_data_dir().parents[1]
        / "venv"
        / "lib"
        / "site-packages"
        / "oddsfox_catalogue"
        / "current_config.py"
    )
    monkeypatch.setattr(current_config, "__file__", str(outside_origin))
    monkeypatch.chdir(tmp_path)

    assert current_config.repository_data_dir() is None
    assert load_current_settings(env={}).root == tmp_path / "data" / "catalogue-v3"
    assert data_path(tmp_path / "outside") == tmp_path / "outside"


def test_checkout_data_path_rejects_siblings_runtime_and_external_paths():
    data_dir = repository_data_dir()
    assert data_dir is not None
    repo = data_dir.parent

    with pytest.raises(CurrentError, match="repository data directory"):
        data_path(data_dir)
    for path in (
        repo / "data-other" / "root",
        data_dir / "catalogue-runtime",
        data_dir / "catalogue-runtime" / "root",
        repo.parent / "external",
    ):
        with pytest.raises(CurrentError, match="repository data directory"):
            data_path(path)


def test_data_path_preserves_traversal_and_symlink_rejection(tmp_path):
    data_dir = repository_data_dir()
    assert data_dir is not None
    with pytest.raises(CurrentError, match="parent traversal"):
        data_path(tmp_path / ".." / "outside")

    link = tmp_path / "link"
    link.symlink_to(data_dir / "tmp", target_is_directory=True)
    with pytest.raises(CurrentError, match="symlink"):
        data_path(link / "root")


def test_validate_root_and_catalogue_root_environment_use_data_boundary(tmp_path):
    outside = repository_data_dir().parent / "data-other" / "root"
    with pytest.raises(CurrentError, match="repository data directory"):
        validate_root(outside, initialize=True)
    with pytest.raises(CurrentError, match="repository data directory"):
        load_current_settings(env={"CATALOGUE_ROOT": str(outside)})
    assert not outside.exists()


@pytest.mark.parametrize("invalid", ["producer", "output"])
def test_refresh_metadata_rejects_invalid_storage_before_client_or_writes(
    tmp_path, monkeypatch, invalid
):
    data_dir = repository_data_dir()
    assert data_dir is not None
    producer = tmp_path / "producer"
    output = tmp_path / "bundle"
    if invalid == "producer":
        producer = data_dir.parent / "data-other" / "producer"
    else:
        output = data_dir.parent / "data-other" / "bundle"

    def fail_if_created(*_args, **_kwargs):
        raise AssertionError("HTTP client must not be created for invalid storage paths")

    monkeypatch.setattr(metadata, "GammaClient", fail_if_created)
    with pytest.raises(metadata.MetadataError, match="path is unsafe"):
        metadata.refresh_metadata(
            gamma=None,  # Path validation must run before endpoint or client setup.
            catalogue=None,
            producer_root=producer,
            market_ids=["11"],
            output=output,
        )
    assert not producer.exists()
    assert not output.exists()


def test_write_bundle_rejects_external_output_before_creation():
    outside = repository_data_dir().parent / "data-other" / "bundle"
    with pytest.raises(metadata.MetadataError, match="output path is unsafe"):
        metadata._write_bundle(outside, {}, "")
    assert not outside.exists()


def test_catalogue_identity_rejects_external_root_before_store_or_lock_access(
    tmp_path, monkeypatch
):
    _data_anchor, catalogue = existing_external_catalogue(tmp_path, monkeypatch)

    def fail_if_accessed(*_args, **_kwargs):
        raise AssertionError("invalid catalogue root must be rejected before lock/store access")

    monkeypatch.setattr(metadata, "run_lock", fail_if_accessed)
    monkeypatch.setattr(metadata, "connect_store", fail_if_accessed)
    with pytest.raises(CurrentError, match="repository data directory"):
        metadata._catalogue_identity(catalogue, ["11"])
    assert catalogue.store_path.exists()
    assert not catalogue.run_lock_path.exists()


def test_refresh_metadata_rejects_external_catalogue_before_client_or_raw_writes(
    tmp_path, monkeypatch
):
    data_anchor, catalogue = existing_external_catalogue(tmp_path, monkeypatch)
    producer = data_anchor / "producer"
    output = data_anchor / "bundle"

    def fail_if_created(*_args, **_kwargs):
        raise AssertionError("invalid catalogue root must be rejected before client creation")

    monkeypatch.setattr(metadata, "GammaClient", fail_if_created)
    with pytest.raises(metadata.MetadataError, match="catalogue path is unsafe"):
        metadata.refresh_metadata(
            gamma=None,
            catalogue=catalogue,
            producer_root=producer,
            market_ids=["11"],
            output=output,
        )
    assert catalogue.store_path.exists()
    assert not catalogue.run_lock_path.exists()
    assert not producer.exists()
    assert not output.exists()


@pytest.mark.parametrize("output_kind", ["external", "case-variant-runtime"])
def test_cli_metadata_rejects_invalid_output_before_lock_or_client(
    tmp_path, monkeypatch, capsys, output_kind
):
    data_dir = repository_data_dir()
    assert data_dir is not None
    output = (
        data_dir.parent / "data-other" / "bundle"
        if output_kind == "external"
        else data_dir / "CATALOGUE-RUNTIME" / "bundle"
    )
    monkeypatch.setattr(
        cli,
        "run_lock",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("invalid output must be rejected before taking a lock")
        ),
    )
    monkeypatch.setattr(
        metadata,
        "GammaClient",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("invalid output must be rejected before creating a client")
        ),
    )

    code = cli.main(
        [
            "--root",
            str(tmp_path / "producer"),
            "metadata",
            "refresh",
            "--market-id",
            "11",
            "--output",
            str(output),
        ]
    )

    assert code == 3
    assert "repository data directory" in capsys.readouterr().out
    assert not output.exists()
