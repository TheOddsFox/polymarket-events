from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from oddsfox_catalogue import cli, current_config, current_runtime, metadata


def test_native_metadata_preflight_precedes_lock_and_http(tmp_path, monkeypatch, capsys):
    producer = SimpleNamespace(root=tmp_path / "producer", run_lock_path=tmp_path / "lock")
    args = SimpleNamespace(root=producer.root, catalogue_root=None, output=tmp_path / "output")
    calls = []
    monkeypatch.setattr(cli, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(current_config, "load_current_settings", lambda _root: producer)

    def reject(ancestor):
        calls.append(ancestor)
        raise OSError("volume flush denied")

    def forbidden_lock(_path):
        raise AssertionError("lock opened before Windows preflight")

    monkeypatch.setattr(cli, "require_durability", reject)
    monkeypatch.setattr(cli, "run_lock", forbidden_lock)
    assert cli._metadata(args) == 3
    assert calls == [tmp_path]
    assert not producer.root.exists()
    assert "volume flush denied" in capsys.readouterr().out


def test_native_metadata_creates_fresh_root_before_pinning_and_locking(
    tmp_path, monkeypatch, capsys
):
    producer = SimpleNamespace(root=tmp_path / "producer", run_lock_path=tmp_path / "lock")
    args = SimpleNamespace(root=producer.root, catalogue_root=None, output=tmp_path / "output")
    calls = []
    monkeypatch.setattr(cli, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(current_config, "load_current_settings", lambda _root: producer)
    monkeypatch.setattr(cli, "require_durability", lambda path: calls.append(("preflight", path)))

    @contextmanager
    def pin(path):
        calls.append(("ancestor", path))
        yield

    @contextmanager
    def root_guard(settings, operation):
        assert settings.root.is_dir()
        assert operation == "metadata"
        calls.append(("root", settings.root))
        yield

    def stop_at_lock(path):
        assert calls[-1] == ("root", producer.root)
        calls.append(("lock", path))
        raise OSError("stop before HTTP")

    monkeypatch.setattr(cli, "pinned_path", pin)
    monkeypatch.setattr(current_runtime, "_windows_root_guard", root_guard)
    monkeypatch.setattr(cli, "run_lock", stop_at_lock)
    assert cli._metadata(args) == 3
    assert calls == [
        ("preflight", tmp_path),
        ("ancestor", tmp_path),
        ("root", producer.root),
        ("lock", producer.run_lock_path),
    ]
    assert "stop before HTTP" in capsys.readouterr().out


def test_missing_volume_ancestor_fails_instead_of_looping():
    class MissingVolume:
        parent = property(lambda self: self)

        def exists(self):
            return False

    with pytest.raises(metadata.MetadataError, match="no existing filesystem ancestor"):
        metadata._existing_ancestor(MissingVolume())
