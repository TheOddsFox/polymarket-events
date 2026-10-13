"""Source seals bind exact retained evidence and a consistent, bounded SQLite snapshot."""

import hashlib
import json
import os
import shutil
import sqlite3
from dataclasses import replace

import pytest

from oddsfox_catalogue import backup, rebuild
from oddsfox_catalogue.backup import _snapshot_ledger
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded
from oddsfox_catalogue.limits import directory_bytes


@pytest.fixture
def sealed_source(tmp_path, monkeypatch):
    settings = Settings(tmp_path / "source")
    settings = replace(
        settings,
        capture=replace(
            settings.capture, max_retained_bytes=128 * 1024**2, max_temp_bytes=128 * 1024**2
        ),
    )
    settings.raw_dir.mkdir(parents=True)
    (settings.raw_dir / "capture.json").write_bytes(b'{"rows":[1]}')
    (settings.raw_dir / "empty-directory").mkdir()
    settings.published_dir.mkdir(parents=True)
    (settings.published_dir / "current.json").write_bytes(b'{"release":"first"}')
    old_release = settings.published_dir / "releases" / "old"
    old_release.mkdir(parents=True)
    (old_release / "release.json").write_bytes(b'{"retained":true}')
    settings.warehouse_path.parent.mkdir(parents=True)
    # Sealing reads exact files; it must not re-open DuckDB to compute semantic records.
    settings.warehouse_path.write_bytes(b"warehouse-v4")
    settings.state_dir.mkdir(parents=True)
    connection = sqlite3.connect(settings.ledger_path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA wal_autocheckpoint=0")
    connection.execute("CREATE TABLE state (id INTEGER PRIMARY KEY, value TEXT)")
    connection.execute("INSERT INTO state VALUES (1, 'original')")
    connection.commit()
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    assert settings.ledger_path.stat().st_size == 8192
    monkeypatch.setattr(rebuild, "read_build_validity", lambda settings: None)
    monkeypatch.setattr(rebuild, "_runtime_revision", lambda: "frozen-runtime")
    monkeypatch.setattr(rebuild, "_model_revision", lambda settings: "frozen-models")
    try:
        yield settings, connection
    finally:
        connection.close()


@pytest.mark.parametrize(
    "change",
    [
        "raw-content",
        "raw-add",
        "raw-remove",
        "raw-empty-dir",
        "pointer",
        "retained-release",
        "warehouse",
        "warehouse-wal",
        "configuration",
    ],
)
def test_seal_detects_exact_source_membership_content_and_configuration(sealed_source, change):
    settings, _ = sealed_source
    before = rebuild._source_seal(settings)
    if change == "raw-content":
        (settings.raw_dir / "capture.json").write_bytes(b'{"rows":[2]}')
    elif change == "raw-add":
        (settings.raw_dir / "extra.json").write_bytes(b"[]")
    elif change == "raw-remove":
        (settings.raw_dir / "capture.json").unlink()
    elif change == "raw-empty-dir":
        (settings.raw_dir / "empty-directory").rename(settings.raw_dir / "renamed-directory")
    elif change == "pointer":
        (settings.published_dir / "current.json").write_bytes(b'{"release":"other"}')
    elif change == "retained-release":
        (settings.published_dir / "releases" / "old" / "release.json").write_bytes(b"changed")
    elif change == "warehouse":
        settings.warehouse_path.write_bytes(b"warehouse-v5")
    elif change == "warehouse-wal":
        settings.warehouse_path.with_name(settings.warehouse_path.name + ".wal").write_bytes(
            b"committed"
        )
    else:
        settings = replace(settings, load=replace(settings.load, duckdb_threads=1))
    assert rebuild._source_seal(settings) != before


def test_seal_excludes_scratch_logs_jobs_and_registration_but_keeps_static_context(sealed_source):
    settings, _ = sealed_source
    before = rebuild._source_seal(settings)
    for path in (
        settings.temporary_dir / "query-spill",
        settings.dlt_pipelines_dir / "completed-job",
        settings.state_dir / "logs" / "run.log",
        settings.state_dir / "rebuild" / "proof" / "rebuilt.bin",
        settings.state_dir / "rebuild" / rebuild.REGISTRATIONS / "registration.json",
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"excluded operational artifact")
    assert rebuild._source_seal(settings) == before
    assert before["context"]["configuration"]["capture"]["max_temp_bytes"] == 128 * 1024**2
    query = replace(settings, capture=replace(settings.capture, max_temp_bytes=64 * 1024**2))
    assert rebuild._source_seal(settings, query_settings=query) == before
    assert not list(settings.temporary_dir.glob("rebuild-ledger-*"))


def test_absent_published_tree_is_sealed_and_later_publication_changes_it(sealed_source):
    settings, _ = sealed_source
    shutil.rmtree(settings.published_dir)
    before = rebuild._source_seal(settings)
    assert before["published"] is None
    assert rebuild._source_seal(settings) == before
    settings.published_dir.mkdir()
    empty_tree = rebuild._source_seal(settings)
    assert empty_tree["published"] is not None and empty_tree != before
    (settings.published_dir / "current.json").write_bytes(b'{"release":"first"}')
    published = rebuild._source_seal(settings)
    assert published != before and published != empty_tree
    assert {k: v for k, v in published.items() if k != "published"} == {
        k: v for k, v in before.items() if k != "published"
    }


@pytest.mark.parametrize(
    "mutation", ["already-read-bytes", "same-size-replacement", "parent-symlink"]
)
def test_streaming_seal_rejects_stable_size_mutations_after_first_read(
    tmp_path, monkeypatch, mutation
):
    parent = tmp_path / "source"
    parent.mkdir()
    path = parent / "evidence"
    original_bytes = b"a" * 64 + b"b" * 128
    path.write_bytes(original_bytes)
    original_inode = path.stat().st_ino
    fdopen = os.fdopen
    reads, closed = [], []

    class MutatingReader:
        def __init__(self, delegate):
            self.delegate = delegate

        def __enter__(self):
            self.delegate.__enter__()
            return self

        def __exit__(self, *args):
            try:
                return self.delegate.__exit__(*args)
            finally:
                closed.append(self.delegate.closed)

        def fileno(self):
            return self.delegate.fileno()

        def read(self, size):
            # Preserve a real file descriptor, but force two chunks with a tiny fixture.
            chunk = self.delegate.read(min(size, 64))
            reads.append(chunk)
            if len(reads) == 1:
                assert chunk == original_bytes[:64]
                if mutation == "already-read-bytes":
                    with path.open("r+b") as writer:
                        writer.write(b"c" * 64)
                elif mutation == "same-size-replacement":
                    replacement = parent / "replacement"
                    replacement.write_bytes(b"c" * len(original_bytes))
                    replacement.replace(path)
                else:
                    retained = tmp_path / "original-source"
                    parent.rename(retained)
                    parent.symlink_to(retained, target_is_directory=True)
                assert path.stat().st_size == len(original_bytes)
            return chunk

    monkeypatch.setattr(
        backup.os, "fdopen", lambda *args, **kwargs: MutatingReader(fdopen(*args, **kwargs))
    )
    with pytest.raises(ValueError, match="changed during streaming|symlink"):
        backup._stream_file(path, max_bytes=len(original_bytes))
    assert closed == [True]
    assert b"".join(reads) == original_bytes
    if mutation == "already-read-bytes":
        assert path.read_bytes() == b"c" * 64 + original_bytes[64:]
    elif mutation == "same-size-replacement":
        assert path.stat().st_ino != original_inode
        assert path.read_bytes() == b"c" * len(original_bytes)
    else:
        assert parent.is_symlink()
        assert path.read_bytes() == original_bytes


def test_ledger_seal_is_stable_across_checkpoint_and_reopen_but_changes_on_commit(sealed_source):
    settings, connection = sealed_source
    initial = rebuild._source_seal(settings)
    connection.execute("UPDATE state SET value='committed in WAL' WHERE id=1")
    connection.commit()
    assert settings.ledger_path.with_name(settings.ledger_path.name + "-wal").stat().st_size > 0
    committed = rebuild._source_seal(settings)
    assert committed["ledger"] != initial["ledger"]
    assert {k: v for k, v in committed.items() if k != "ledger"} == {
        k: v for k, v in initial.items() if k != "ledger"
    }
    connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    assert rebuild._source_seal(settings) == committed
    connection.close()
    with sqlite3.connect(settings.ledger_path) as reopened:
        assert reopened.execute("SELECT value FROM state").fetchone() == ("committed in WAL",)
    assert rebuild._source_seal(settings) == committed


def test_uncommitted_sqlite_writer_is_not_part_of_consistent_seal(sealed_source):
    settings, connection = sealed_source
    before = rebuild._source_seal(settings)
    connection.execute("UPDATE state SET value='uncommitted' WHERE id=1")
    assert rebuild._source_seal(settings) == before
    connection.rollback()
    assert rebuild._source_seal(settings) == before


@pytest.mark.parametrize(
    "path_kind", ["raw", "published", "warehouse", "warehouse-wal", "ledger-wal"]
)
def test_seal_rejects_symlinks_without_retaining_a_snapshot(sealed_source, tmp_path, path_kind):
    settings, _ = sealed_source
    outside = tmp_path / "outside"
    outside.write_bytes(b"outside retained evidence")
    paths = {
        "raw": settings.raw_dir / "alias",
        "published": settings.published_dir / "alias",
        "warehouse": settings.warehouse_path,
        "warehouse-wal": settings.warehouse_path.with_name(settings.warehouse_path.name + ".wal"),
        "ledger-wal": settings.ledger_path.with_name(settings.ledger_path.name + "-wal"),
    }
    path = paths[path_kind]
    if path.exists():
        path.unlink()
    path.symlink_to(outside)
    with pytest.raises(ValueError, match="symlink"):
        rebuild._source_seal(settings)
    assert outside.read_bytes() == b"outside retained evidence"
    assert not list(settings.temporary_dir.glob("rebuild-ledger-*"))


def test_seal_checks_temporary_allowance_before_creating_snapshot(sealed_source):
    settings, _ = sealed_source
    limited = replace(settings, capture=replace(settings.capture, max_temp_bytes=8191))
    with pytest.raises(ValueError, match="temporary allowance"):
        rebuild._source_seal(limited)
    assert not settings.temporary_dir.exists()


@pytest.mark.parametrize("nested_scratch", [False, True])
def test_external_dlt_and_scratch_reserve_snapshot_bytes_without_raising_declared_cap(
    sealed_source, tmp_path, monkeypatch, nested_scratch
):
    settings, _ = sealed_source
    working = tmp_path / "external-dlt"
    working.mkdir()
    (working / "retained-job").write_bytes(b"d" * 4096)
    monkeypatch.setattr(
        Settings,
        "dlt_pipelines_dir",
        property(
            lambda current: working if current.root == settings.root else current.state_dir / "dlt"
        ),
    )
    source_bytes = directory_bytes(settings.root) + 4096
    scratch = (
        settings.state_dir / "rebuild" / "proof"
        if nested_scratch
        else tmp_path / "external-scratch"
    )
    scratch.mkdir(parents=True)
    (scratch / "retained-unit").write_bytes(b"s" * 4096)
    rebuilt = rebuild.scratch_settings(settings, scratch)
    union_bytes = source_bytes + 4096
    exact = replace(settings, capture=replace(settings.capture, max_retained_bytes=union_bytes))
    rebuild._combined_limits(exact, rebuilt)
    with pytest.raises(ValueError, match="combined retained"):
        rebuild._combined_limits(
            replace(exact, capture=replace(exact.capture, max_retained_bytes=union_bytes - 1)),
            rebuilt,
        )

    # One byte below the full root union plus an 8,192-byte ledger snapshot must reject.
    bounded = replace(
        settings, capture=replace(settings.capture, max_retained_bytes=union_bytes + 8191)
    )
    query = rebuild._source_query_settings(bounded, rebuilt)
    assert query.capture.max_retained_bytes == bounded.capture.max_retained_bytes - (
        0 if nested_scratch else 4096
    )
    assert query.capture.max_retained_bytes <= bounded.capture.max_retained_bytes
    with pytest.raises(RequestBudgetExceeded, match="retained storage"):
        rebuild._source_seal(bounded, query_settings=query)
    assert not settings.temporary_dir.exists()

    admitted = replace(
        bounded, capture=replace(bounded.capture, max_retained_bytes=union_bytes + 8192)
    )
    seal = rebuild._source_seal(
        admitted, query_settings=rebuild._source_query_settings(admitted, rebuilt)
    )
    assert seal["ledger"]["bytes"] == 8192
    assert seal["context"]["configuration"]["capture"]["max_retained_bytes"] == union_bytes + 8192
    assert (working / "retained-job").read_bytes() == b"d" * 4096
    assert (scratch / "retained-unit").read_bytes() == b"s" * 4096


def test_bounded_ledger_snapshot_exact_boundary_preserves_source_and_wal(sealed_source, tmp_path):
    settings, connection = sealed_source
    connection.execute("UPDATE state SET value='new committed value' WHERE id=1")
    connection.commit()
    source_bytes = settings.ledger_path.read_bytes()
    wal = settings.ledger_path.with_name(settings.ledger_path.name + "-wal")
    wal_bytes = wal.read_bytes()
    rejected = tmp_path / "too-small.sqlite"
    with pytest.raises(ValueError, match="byte allowance"):
        _snapshot_ledger(settings.ledger_path, rejected, max_bytes=8191)
    assert not rejected.exists()
    copied = tmp_path / "exact.sqlite"
    _snapshot_ledger(settings.ledger_path, copied, max_bytes=8192)
    assert copied.stat().st_size == 8192
    with sqlite3.connect(copied) as snapshot:
        assert snapshot.execute("SELECT * FROM state").fetchall() == [(1, "new committed value")]
        assert snapshot.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    assert settings.ledger_path.read_bytes() == source_bytes
    assert wal.read_bytes() == wal_bytes
    with pytest.raises(FileExistsError, match="fresh destination"):
        _snapshot_ledger(settings.ledger_path, copied, max_bytes=8192)
    with sqlite3.connect(copied) as snapshot:
        assert snapshot.execute("SELECT * FROM state").fetchall() == [(1, "new committed value")]


def test_bounded_snapshot_pins_preflight_state_when_writer_commits_before_backup(
    sealed_source, tmp_path, monkeypatch
):
    settings, writer = sealed_source
    connect = sqlite3.connect
    commits = []

    class CommitDuringBackup(sqlite3.Connection):
        def backup(self, target, *args, **kwargs):
            # The write lands after source page-count admission and before copying any pages.
            writer.execute("INSERT INTO state VALUES (2, ?)", ["x" * 65536])
            writer.commit()
            commits.append(True)
            return super().backup(target, *args, **kwargs)

    def snapshot_connection(*args, **kwargs):
        if kwargs.get("uri") and "?mode=ro" in str(args[0]):
            kwargs["factory"] = CommitDuringBackup
        return connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", snapshot_connection)
    target = tmp_path / "pinned.sqlite"
    _snapshot_ledger(settings.ledger_path, target, max_bytes=8192)
    assert commits == [True]
    assert target.stat().st_size == 8192
    with connect(target) as snapshot:
        assert snapshot.execute("SELECT * FROM state").fetchall() == [(1, "original")]
        assert snapshot.execute("PRAGMA integrity_check").fetchone() == ("ok",)
    assert writer.execute("SELECT id, length(value) FROM state ORDER BY id").fetchall() == [
        (1, 8),
        (2, 65536),
    ]
    assert writer.execute("PRAGMA page_count").fetchone()[0] * 4096 > 8192


@pytest.mark.parametrize("limit", [True, False, 8192.0, "8192", 0, -1])
def test_invalid_snapshot_allowances_fail_before_destination_creation(
    sealed_source, tmp_path, limit
):
    settings, _ = sealed_source
    target = tmp_path / "invalid.sqlite"
    with pytest.raises(ValueError, match="positive"):
        _snapshot_ledger(settings.ledger_path, target, max_bytes=limit)
    assert not target.exists()


def test_scratch_local_replacement_registration_cannot_reset_a_changed_source(
    sealed_source, tmp_path, monkeypatch
):
    settings, _ = sealed_source
    scratch = tmp_path / "proof"
    # Only source validation is synthetic here; anchor and exact byte sealing are real.
    monkeypatch.setattr(rebuild, "_source_binding", lambda *args, **kwargs: {"runtime": "frozen"})
    assert rebuild.rebuild_and_verify(settings, scratch=scratch, step=True).pending
    anchor, _ = rebuild._registration_path(settings, scratch)
    original = anchor.read_bytes()
    (settings.raw_dir / "capture.json").write_bytes(b'{"rows":[2]}')
    forged = json.loads(original)
    forged["seal"] = rebuild._source_seal(settings)
    forged_body = json.dumps(forged).encode()
    (scratch / "replacement-registration.json").write_bytes(forged_body)
    record = json.loads((scratch / rebuild.REBUILD_RECORD).read_bytes())
    record["registration"]["sha256"] = hashlib.sha256(forged_body).hexdigest()
    record.pop("sha256")
    record["sha256"] = rebuild.json_descriptor(record)["sha256"]
    (scratch / rebuild.REBUILD_RECORD).write_text(json.dumps(record))
    with pytest.raises(ValueError, match="registration checksum"):
        rebuild._validated_rebuild_source(settings, scratch)
    assert anchor.read_bytes() == original
    assert rebuild._read_record(scratch)["phase"] == "initialization"
