import uuid
from pathlib import Path

import pytest

from oddsfox_catalogue import cli, current_config, metadata
from oddsfox_catalogue.current_contract import CurrentError


def _fake_checkout(tmp_path, monkeypatch, *, installed=False):
    repo = tmp_path / "checkout"
    (repo / "pyproject.toml").parent.mkdir(parents=True)
    (repo / "pyproject.toml").write_text(
        '[project]\nname = "oddsfox-catalogue"\nversion = "0.0.0"\n'
    )
    source = repo / "src/oddsfox_catalogue/current_config.py"
    source.parent.mkdir(parents=True)
    source.write_text("# source checkout marker\n")
    if installed:
        origin = (
            repo
            / "data/catalogue-runtime/venv/lib/python3.12/site-packages/"
            / "oddsfox_catalogue/current_config.py"
        )
        origin.parent.mkdir(parents=True)
        origin.write_text("# installed package marker\n")
    else:
        origin = source
    monkeypatch.setattr(current_config, "__file__", str(origin))
    return repo, repo / "data"


@pytest.mark.parametrize("installed", [False, True], ids=["source", "wheel-under-runtime"])
def test_repository_discovery_uses_package_origin_inside_matching_checkout(
    tmp_path, monkeypatch, installed
):
    repo, data_dir = _fake_checkout(tmp_path, monkeypatch, installed=installed)
    monkeypatch.chdir(tmp_path)

    assert current_config.repository_data_dir() == data_dir
    assert current_config.load_current_settings(env={}).root == data_dir / "catalogue-v3"


def test_external_install_keeps_portable_cwd_default_even_when_cwd_is_a_checkout(
    tmp_path, monkeypatch
):
    repo, _data_dir = _fake_checkout(tmp_path, monkeypatch)
    external_origin = (
        Path("/tmp")
        / f"catalogue-installed-{uuid.uuid4().hex}"
        / "site-packages/oddsfox_catalogue/current_config.py"
    )
    monkeypatch.setattr(current_config, "__file__", str(external_origin))
    monkeypatch.chdir(repo)

    assert current_config.repository_data_dir() is None
    assert current_config.load_current_settings(env={}).root == repo / "data/catalogue-v3"


def test_malformed_matching_checkout_metadata_blocks_portable_path_fallback(tmp_path, monkeypatch):
    _repo, data_dir = _fake_checkout(tmp_path, monkeypatch)
    (data_dir.parent / "pyproject.toml").write_text("[project\n")
    requested_root = data_dir / "catalogue-v3"

    with pytest.raises(CurrentError, match="identify the local catalogue checkout"):
        current_config.load_current_settings(requested_root, env={})

    assert not requested_root.exists()


def test_data_path_allows_only_strict_data_descendants_and_excludes_runtime(tmp_path, monkeypatch):
    _repo, data_dir = _fake_checkout(tmp_path, monkeypatch)

    accepted = data_dir / "nested/catalogue-v3"
    assert current_config.data_path(accepted) == accepted.resolve()
    # Exclusion is a subtree boundary, not a string-prefix rule.
    near_match = data_dir / "catalogue-runtime-old/root"
    assert current_config.data_path(near_match) == near_match.resolve()

    for rejected in (
        data_dir,
        data_dir / "catalogue-runtime",
        data_dir / "catalogue-runtime/venv/root",
        data_dir.parent / "data-other/root",
        tmp_path / "outside/root",
    ):
        with pytest.raises(CurrentError):
            current_config.data_path(rejected)


def test_data_path_rejects_traversal_and_symlink_aliases_inside_data(tmp_path, monkeypatch):
    _repo, data_dir = _fake_checkout(tmp_path, monkeypatch)
    with pytest.raises(CurrentError, match="traversal"):
        current_config.data_path(data_dir / ".." / "outside")

    alias = tmp_path / "data-alias"
    alias.symlink_to(data_dir, target_is_directory=True)
    with pytest.raises(CurrentError, match="symlink"):
        current_config.data_path(alias / "catalogue-v3")


def test_invalid_environment_root_wins_over_malformed_config_without_creating_data(
    tmp_path, monkeypatch
):
    _repo, data_dir = _fake_checkout(tmp_path, monkeypatch)
    invalid_root = data_dir / "catalogue-runtime/escaped-root"
    malformed_config = data_dir / "broken.toml"
    malformed_config.parent.mkdir(parents=True)
    malformed_config.write_text("[catalogue\n")

    with pytest.raises(CurrentError):
        current_config.load_current_settings(
            env={
                "CATALOGUE_ROOT": str(invalid_root),
                "CATALOGUE_CONFIG": str(malformed_config),
            }
        )
    assert not invalid_root.exists()


@pytest.mark.parametrize("invalid", ["producer-runtime", "output-runtime"])
def test_metadata_rejects_runtime_paths_before_http_or_any_directory_creation(
    tmp_path, monkeypatch, invalid
):
    _repo, data_dir = _fake_checkout(tmp_path, monkeypatch)
    producer = data_dir / "producer"
    output = data_dir / "bundle"
    if invalid == "producer-runtime":
        producer = data_dir / "catalogue-runtime/producer"
    else:
        output = data_dir / "catalogue-runtime/bundle"

    def fail_if_http(*_args, **_kwargs):
        pytest.fail("unsafe paths must be rejected before HTTP client construction")

    monkeypatch.setattr(metadata, "GammaClient", fail_if_http)
    with pytest.raises(metadata.MetadataError):
        metadata.refresh_metadata(
            gamma=None,
            catalogue=None,
            producer_root=producer,
            market_ids=["11"],
            output=output,
        )

    assert not producer.exists()
    assert not output.exists()
    assert not (data_dir / "catalogue-runtime").exists()


def test_bundle_writer_rejects_runtime_output_before_staging(tmp_path, monkeypatch):
    _repo, data_dir = _fake_checkout(tmp_path, monkeypatch)
    output = data_dir / "catalogue-runtime/bundle"

    with pytest.raises(metadata.MetadataError):
        metadata._write_bundle(output, {}, "")

    assert not output.exists()
    assert not output.parent.exists()


@pytest.mark.parametrize("selection", ["explicit", "environment"])
def test_cli_metadata_rejects_runtime_producer_before_lock_or_client(
    tmp_path, monkeypatch, capsys, selection
):
    _repo, data_dir = _fake_checkout(tmp_path, monkeypatch)
    producer = data_dir / "catalogue-runtime/producer"
    output = data_dir / "metadata-bundle"
    monkeypatch.setenv("CATALOGUE_ROOT", str(data_dir / "safe-env-root"))

    def fail_if_started(*_args, **_kwargs):
        pytest.fail("invalid producer must be rejected before locking or HTTP")

    monkeypatch.setattr(cli, "run_lock", fail_if_started)
    monkeypatch.setattr(metadata, "GammaClient", fail_if_started)
    argv = [] if selection == "environment" else ["--root", str(producer)]
    if selection == "environment":
        monkeypatch.setenv("CATALOGUE_ROOT", str(producer))
    argv += [
        "metadata",
        "refresh",
        "--market-id",
        "11",
        "--output",
        str(output),
    ]

    assert cli.main(argv) == 3
    assert "blocked" in capsys.readouterr().out
    assert not producer.exists()
    assert not output.exists()


def test_cli_metadata_rejects_external_catalogue_before_lock_client_or_writes(
    tmp_path, monkeypatch, capsys
):
    _repo, data_dir = _fake_checkout(tmp_path, monkeypatch)
    producer = data_dir / "metadata-producer"
    catalogue = data_dir.parent / "data-other/catalogue"
    output = data_dir / "metadata-bundle"

    def fail_if_started(*_args, **_kwargs):
        pytest.fail("external catalogue roots must be rejected before locking or HTTP")

    monkeypatch.setattr(cli, "run_lock", fail_if_started)
    monkeypatch.setattr(metadata, "GammaClient", fail_if_started)
    assert (
        cli.main(
            [
                "--root",
                str(producer),
                "metadata",
                "refresh",
                "--market-id",
                "11",
                "--catalogue-root",
                str(catalogue),
                "--output",
                str(output),
            ]
        )
        == 3
    )

    assert "blocked" in capsys.readouterr().out
    assert not producer.exists()
    assert not catalogue.exists()
    assert not output.exists()
    assert not (producer / "metadata/raw").exists()


@pytest.mark.parametrize("selection", ["explicit", "environment"])
def test_cli_init_rejects_checkout_escape_before_initialization(
    tmp_path, monkeypatch, capsys, selection
):
    _repo, data_dir = _fake_checkout(tmp_path, monkeypatch)
    invalid_root = (
        data_dir.parent / "data-other/root"
        if selection == "explicit"
        else data_dir / "catalogue-runtime/root"
    )
    monkeypatch.setenv("CATALOGUE_ROOT", str(data_dir / "safe-env-root"))

    def fail_if_started(*_args, **_kwargs):
        pytest.fail("invalid root must be rejected before catalogue initialization")

    from oddsfox_catalogue import current_runtime

    monkeypatch.setattr(current_runtime, "supervise", fail_if_started)
    argv = ["--root", str(invalid_root)] if selection == "explicit" else []
    if selection == "environment":
        monkeypatch.setenv("CATALOGUE_ROOT", str(invalid_root))
    argv.append("init")

    assert cli.main(argv) == 3
    assert "blocked" in capsys.readouterr().out
    assert not invalid_root.exists()
