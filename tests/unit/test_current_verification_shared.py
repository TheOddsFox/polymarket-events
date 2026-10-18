"""Shared validation preserves independent release and bound-store assurance."""

import hashlib
import json
import os
from pathlib import Path

import pytest

from oddsfox_catalogue import current_publish as publisher
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentSettings, connect_store, validate_root
from oddsfox_catalogue.current_contract import KEYS, SCHEMAS, CurrentError, projection
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.fingerprints import semantic_fingerprint


@pytest.fixture
def catalogue(tmp_path):
    settings = CurrentSettings(tmp_path / "current")
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        store.initialize(conn)
        store.apply(
            conn,
            project_response(
                [
                    {
                        "id": "1",
                        "title": "published event",
                        "markets": [
                            {
                                "id": "2",
                                "version": "v1",
                                "conditionId": "0x" + "2" * 64,
                                "outcomes": ["Yes", "No"],
                                "clobTokenIds": ["11", "12"],
                                "positionIds": ["21", "22"],
                            },
                            {
                                "id": "3",
                                "version": "v2",
                                "conditionId": None,
                                "outcomes": ["Up", "Down"],
                                "positionIds": ["31", "32"],
                                "clobTokenIds": ["41", "42"],
                            },
                        ],
                    }
                ],
                "events",
                received_at="2026-10-14T00:00:00Z",
                response_id="native-fixture",
                native_market_ids=["2", "3"],
            ),
        )
        assert store.validate_store(conn)["valid"]
        result = publisher.publish(settings, conn)
        yield settings, conn, Path(result["path"])


def replace_column(table, column, expression):
    return (
        "SELECT "
        + ", ".join(
            f'{expression} AS "{name}"' if name == column else f'"{name}"'
            for name, _ in SCHEMAS[table]
        )
        + f" FROM {table}"
    )


def rehashed_release(catalogue, queries=None):
    """Forge fully matching physical hashes, counts and semantic descriptors."""
    settings, conn, original = catalogue
    release = settings.root / "forged-release"
    release.mkdir()
    manifest = json.loads((original / "manifest.json").read_text())
    for table in SCHEMAS:
        query = (queries or {}).get(table, f"SELECT {projection(table)} FROM {table}")
        file = release / f"{table}.parquet"
        conn.execute(f"COPY ({query}) TO ? (FORMAT PARQUET)", [str(file)])
        digest = semantic_fingerprint(conn, f"SELECT * FROM read_parquet('{file}')")
        manifest["relations"][table] = {
            **digest,
            "sha256": hashlib.sha256(file.read_bytes()).hexdigest(),
            "bytes": file.stat().st_size,
        }
        manifest["counts"][table] = digest["rows"]
    (release / "manifest.json").write_text(json.dumps(manifest))
    return release, manifest


@pytest.mark.parametrize("table", SCHEMAS)
@pytest.mark.parametrize("mutation", ["duplicate", "null"])
def test_equal_corrupt_source_and_export_still_reject_public_keys(catalogue, table, mutation):
    settings, conn, _ = catalogue
    pointer = settings.published_dir / "current.json"
    before = pointer.read_bytes()
    # Simulate a persisted table whose PK/NOT NULL constraints were lost.
    conn.execute(f"CREATE TABLE damaged AS SELECT * FROM {table}")
    conn.execute(f"DROP TABLE {table}")
    conn.execute(f"ALTER TABLE damaged RENAME TO {table}")
    if mutation == "duplicate":
        conn.execute(f"INSERT INTO {table} SELECT * FROM {table} ORDER BY ALL LIMIT 1")
    else:
        conn.execute(f'UPDATE {table} SET "{KEYS[table][-1]}"=NULL')
    release, manifest = rehashed_release(catalogue)
    for name in SCHEMAS:
        expected = semantic_fingerprint(conn, f"SELECT {projection(name)} FROM {name}")
        assert all(manifest["relations"][name][key] == value for key, value in expected.items())
    message = f"{mutation} {table} key"
    with pytest.raises(CurrentError, match=message):
        store.validate_store(conn)
    with pytest.raises(CurrentError, match=message):
        publisher.verify_release(release, connection=conn)
    with pytest.raises(CurrentError, match=message):
        publisher.verify_release(release, settings=settings)
    with pytest.raises(CurrentError, match=message):
        publisher.publish(settings, conn)
    assert pointer.read_bytes() == before
    assert publisher.verify_current(settings)["valid"]


@pytest.mark.parametrize("market_id", ["2", "3"], ids=["v1", "v2"])
@pytest.mark.parametrize(
    "corruption",
    ["event-fk", "market-fk", "outcome-fk", "selected-alias", "condition", "ordinal", "empty"],
)
def test_rehashed_standalone_release_rejects_relational_and_native_corruption(
    catalogue, market_id, corruption
):
    settings, conn, _ = catalogue
    if corruption in {"event-fk", "market-fk"}:
        table = "event_markets"
        column = "event_id" if corruption == "event-fk" else "market_id"
        query = replace_column(
            table, column, f"CASE WHEN market_id='{market_id}' THEN '999' ELSE {column} END"
        )
    elif corruption == "empty":
        table = "outcomes"
        query = f"SELECT * FROM outcomes WHERE market_id<>'{market_id}'"
    else:
        table = "outcomes"
        column, expression = {
            "outcome-fk": ("market_id", "'999'"),
            "selected-alias": ("asset_id", "'999'"),
            "condition": ("condition_id", "'0x" + "f" * 64 + "'"),
            "ordinal": ("outcome_index", "outcome_index+1"),
        }[corruption]
        query = replace_column(
            table, column, f"CASE WHEN market_id='{market_id}' THEN {expression} ELSE {column} END"
        )
    release, _ = rehashed_release(catalogue, {table: query})
    with pytest.raises(CurrentError, match="integrity"):
        publisher.verify_release(release, settings=settings)
    # A malformed independent artifact does not invalidate the working store.
    assert store.validate_store(conn)["valid"]


def corrupt_private(conn, corruption):
    if corruption == "missing-accepted-claim":
        conn.execute("DELETE FROM _identity_claims WHERE market_id='2' AND role='accepted'")
    elif corruption == "accepted-owner":
        mapping = conn.execute("SELECT accepted FROM _identity WHERE market_id='3'").fetchone()[0]
        conn.execute("UPDATE _identity SET accepted=? WHERE market_id='2'", [mapping])
    elif corruption == "hidden-reservation":
        conn.execute(
            "INSERT INTO _identity_claims VALUES ('polymarket','999','accepted','ctf_token','11')"
        )
    else:
        conn.execute(
            "UPDATE _identity SET invalidated_at='2026-10-14T01:00:00Z' WHERE market_id='2'"
        )


@pytest.mark.parametrize(
    "corruption", ["missing-accepted-claim", "accepted-owner", "hidden-reservation", "invalidated"]
)
def test_equal_public_export_cannot_hide_private_corruption(catalogue, corruption):
    settings, conn, release = catalogue
    before = (settings.published_dir / "current.json").read_bytes()
    public_before = {
        table: semantic_fingerprint(conn, f"SELECT * FROM {table}") for table in SCHEMAS
    }
    corrupt_private(conn, corruption)
    assert {
        table: semantic_fingerprint(conn, f"SELECT * FROM {table}") for table in SCHEMAS
    } == public_before
    assert publisher.verify_release(release, settings=settings)["valid"]
    assert publisher._verify_release_on_connection(release, conn, bind_store=False)["valid"]
    with pytest.raises(CurrentError):
        publisher.verify_release(release, connection=conn)
    with pytest.raises(CurrentError):
        publisher._verify_release_on_connection(release, conn, bind_store=True)
    with pytest.raises(CurrentError):
        publisher.publish(settings, conn)
    assert (settings.published_dir / "current.json").read_bytes() == before


def test_identical_public_content_does_not_bypass_generation_binding(catalogue):
    settings, conn, release = catalogue
    old_generation = store.generation(conn)
    store.set_state(conn, "generation", old_generation + 1)
    assert publisher._verify_release_on_connection(release, conn, bind_store=False)["valid"]
    with pytest.raises(CurrentError, match="generation differs"):
        publisher.verify_release(release, connection=conn)
    with pytest.raises(CurrentError, match="generation differs"):
        publisher._verify_release_on_connection(release, conn, bind_store=True)
    result = publisher.verify_current(settings, connection=conn)
    assert result["published_generation"] == old_generation
    assert result["store_generation"] == old_generation + 1


def test_previous_release_can_be_verified_on_borrowed_newer_store_connection(catalogue):
    settings, conn, release = catalogue
    old_generation = store.generation(conn)
    store.apply(
        conn,
        project_response(
            [{"id": "1", "title": "newer working state"}],
            "events",
            received_at="2026-10-14T01:00:00Z",
            response_id="newer-event",
        ),
    )
    assert (
        publisher._verify_release_on_connection(release, conn, bind_store=False)["generation"]
        == old_generation
    )
    with pytest.raises(CurrentError, match="export differs"):
        publisher._verify_release_on_connection(release, conn, bind_store=True)
    with pytest.raises(CurrentError, match="export differs"):
        publisher.verify_release(release, connection=conn)
    assert (
        publisher.verify_current(settings, connection=conn)["published_generation"]
        == old_generation
    )
    assert store.validate_store(conn)["valid"]


def count_public_checks(monkeypatch):
    calls = []
    original = store.validate_public

    def checked(conn, sources):
        calls.append(dict(sources))
        return original(conn, sources)

    monkeypatch.setattr(store, "validate_public", checked)
    monkeypatch.setattr(publisher, "_integrity", checked)
    return calls


@pytest.mark.parametrize("api", ["release", "current", "noop", "borrowed"])
def test_each_bound_verification_and_noop_checks_public_data_exactly_once(
    catalogue, monkeypatch, api
):
    settings, conn, release = catalogue
    calls = count_public_checks(monkeypatch)
    for _ in range(2):
        calls.clear()
        if api == "release":
            result = publisher.verify_release(release, connection=conn)
        elif api == "current":
            result = publisher.verify_current(settings, connection=conn)
        elif api == "noop":
            result = publisher.publish(settings, conn)
            assert result["noop"]
        else:
            result = publisher._verify_release_on_connection(release, conn, bind_store=True)
        assert result["valid"]
        assert calls == [{table: table for table in SCHEMAS}]


@pytest.mark.parametrize("api", ["release", "current"])
def test_later_bound_public_api_rechecks_private_state_without_cached_success(
    catalogue, monkeypatch, api
):
    settings, conn, release = catalogue
    calls = count_public_checks(monkeypatch)

    def verify():
        if api == "release":
            return publisher.verify_release(release, connection=conn)
        return publisher.verify_current(settings, connection=conn)

    assert verify()["valid"]
    corrupt_private(conn, "missing-accepted-claim")
    with pytest.raises(CurrentError, match="integrity"):
        verify()
    assert calls == [{table: table for table in SCHEMAS}] * 2


@pytest.mark.parametrize("api", ["release", "current"])
def test_later_unbound_public_api_rechecks_resealed_parquet_semantics(catalogue, monkeypatch, api):
    settings, conn, release = catalogue
    calls = count_public_checks(monkeypatch)

    def verify():
        if api == "release":
            return publisher.verify_release(release, settings=settings)
        return publisher.verify_current(settings)

    assert verify()["valid"]
    file = release / "event_markets.parquet"
    conn.execute(
        "COPY (SELECT venue,'999' AS event_id,market_id FROM event_markets) TO ? (FORMAT PARQUET)",
        [str(file)],
    )
    manifest_path = release / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["relations"]["event_markets"] = {
        **semantic_fingerprint(conn, f"SELECT * FROM read_parquet('{file}')"),
        "sha256": hashlib.sha256(file.read_bytes()).hexdigest(),
        "bytes": file.stat().st_size,
    }
    manifest_path.write_text(json.dumps(manifest))
    pointer_path = settings.published_dir / "current.json"
    pointer = json.loads(pointer_path.read_text())
    pointer["manifest_sha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    pointer_path.write_text(json.dumps(pointer))
    with pytest.raises(CurrentError, match="integrity"):
        verify()
    assert len(calls) == 2
    assert all("read_parquet(" in source for sources in calls for source in sources.values())


def domain_snapshot(conn):
    return {
        table: conn.execute(f'SELECT * FROM "{table}" ORDER BY ALL').fetchall()
        for (table,) in conn.execute("SHOW TABLES").fetchall()
    }


@pytest.fixture
def candidate(catalogue):
    settings, conn, release = catalogue
    store.apply(
        conn,
        project_response(
            [{"id": "1", "title": "committed refresh awaiting publication"}],
            "events",
            received_at="2026-10-14T01:00:00Z",
            response_id="committed-refresh",
        ),
    )
    assert store.generation(conn) == 2
    return (
        settings,
        conn,
        release,
        (settings.published_dir / "current.json").read_bytes(),
        domain_snapshot(conn),
    )


def assert_failed_candidate_preserved_committed_state(candidate):
    settings, conn, release, pointer, before = candidate
    assert (settings.published_dir / "current.json").read_bytes() == pointer
    assert domain_snapshot(conn) == before
    # Reuse the bounded connection: an injected FIFO/symlink in the failed stage
    # intentionally makes whole-root storage admission reject further operations.
    assert publisher._verify_release_on_connection(release, conn, bind_store=False)["valid"]
    assert {path.name for path in (settings.published_dir / "releases").iterdir()} == {release.name}


def mutate_after_manifest_write(monkeypatch, mutation):
    original = publisher.atomic_write_bytes
    calls = []

    def tamper(path, data):
        result = original(path, data)
        if path.name == "manifest.json" and path.parent.name.startswith(".staging-"):
            calls.append(path)
            mutation(path, data)
        return result

    monkeypatch.setattr(publisher, "atomic_write_bytes", tamper)
    return calls


@pytest.mark.parametrize("table", SCHEMAS)
def test_candidate_rechecks_each_parquet_after_manifest_write(candidate, monkeypatch, table):
    settings, conn, _, _, _ = candidate

    def tamper(manifest_path, data):
        file = manifest_path.parent / f"{table}.parquet"
        recorded = json.loads(data)["relations"][table]
        assert hashlib.sha256(file.read_bytes()).hexdigest() == recorded["sha256"]
        column, expression = {
            "events": ("title", "title || ':tampered'"),
            "markets": ("question", "coalesce(question,'') || ':tampered'"),
            "event_markets": ("event_id", "'999'"),
            "outcomes": ("outcome_label", "outcome_label || ':tampered'"),
        }[table]
        # COPY creates a valid, readable Parquet with its own updated body/footer.
        conn.execute(
            f"COPY ({replace_column(table, column, expression)}) TO ? (FORMAT PARQUET)",
            [str(file)],
        )
        assert (
            conn.execute("SELECT count(*) FROM read_parquet(?)", [str(file)]).fetchone()[0]
            == recorded["rows"]
        )
        assert hashlib.sha256(file.read_bytes()).hexdigest() != recorded["sha256"]

    calls = mutate_after_manifest_write(monkeypatch, tamper)
    with pytest.raises(CurrentError, match=f"candidate {table} changed after verification"):
        publisher.publish(settings, conn)
    assert len(calls) == 1
    assert_failed_candidate_preserved_committed_state(candidate)


@pytest.mark.parametrize("mutation", ["whitespace", "invalid-json", "replacement"])
def test_candidate_requires_exact_manifest_readback(candidate, monkeypatch, mutation):
    settings, conn, _, _, _ = candidate

    def tamper(path, data):
        if mutation == "whitespace":
            path.write_bytes(data + b"\n")
        elif mutation == "invalid-json":
            path.write_bytes(b"{interrupted")
        else:
            replacement = json.loads(data)
            replacement["coverage"]["complete"] = True
            path.unlink()
            path.write_text(json.dumps(replacement, sort_keys=True))

    calls = mutate_after_manifest_write(monkeypatch, tamper)
    with pytest.raises(CurrentError, match="candidate manifest changed after verification"):
        publisher.publish(settings, conn)
    assert len(calls) == 1
    assert_failed_candidate_preserved_committed_state(candidate)


@pytest.mark.parametrize(
    "mutation",
    [
        "extra-file",
        "extra-directory",
        "extra-symlink",
        "extra-fifo",
        "missing-parquet",
        "parquet-fifo",
    ],
)
def test_candidate_rejects_changed_inventory_and_special_files(candidate, monkeypatch, mutation):
    if mutation in {"extra-fifo", "parquet-fifo"} and not hasattr(os, "mkfifo"):
        pytest.skip("POSIX FIFO contract; native reparse rejection has separate coverage")
    settings, conn, _, _, _ = candidate

    def tamper(path, data):
        stage = path.parent
        if mutation == "extra-file":
            (stage / "unexpected").write_text("unverified")
        elif mutation == "extra-directory":
            (stage / "unexpected").mkdir()
        elif mutation == "extra-symlink":
            (stage / "unexpected").symlink_to(stage / "events.parquet")
        elif mutation == "extra-fifo":
            os.mkfifo(stage / "unexpected")
        else:
            file = stage / "events.parquet"
            file.unlink()
            if mutation == "parquet-fifo":
                os.mkfifo(file)

    calls = mutate_after_manifest_write(monkeypatch, tamper)
    message = "special file" if mutation == "parquet-fifo" else "candidate inventory changed"
    with pytest.raises(CurrentError, match=message):
        publisher.publish(settings, conn)
    assert len(calls) == 1
    assert_failed_candidate_preserved_committed_state(candidate)


def test_candidate_rechecks_generation_after_inspection(candidate, monkeypatch):
    settings, conn, _, _, _ = candidate

    def tamper(path, data):
        manifest = json.loads(data)
        assert manifest["generation"] == store.generation(conn)
        store.set_state(conn, "generation", manifest["generation"] + 1)

    calls = mutate_after_manifest_write(monkeypatch, tamper)
    with pytest.raises(CurrentError, match="candidate source generation changed"):
        publisher.publish(settings, conn)
    assert len(calls) == 1
    assert_failed_candidate_preserved_committed_state(candidate)


def test_candidate_inspects_once_with_nine_fingerprints_then_external_verification_is_fresh(
    candidate, monkeypatch
):
    settings, conn, previous, _, _ = candidate
    calls = []
    inspections = []
    fingerprint = semantic_fingerprint
    inspect = publisher._inspect_relations

    def measured(connection, query, parameters=None):
        calls.append(query)
        return fingerprint(connection, query, parameters)

    def inspected(path, connection, *, bind_store, expected=None):
        start = len(calls)
        result = inspect(path, connection, bind_store=bind_store, expected=expected)
        inspections.append((Path(path), bind_store, calls[start:]))
        return result

    monkeypatch.setattr(publisher, "semantic_fingerprint", measured)
    monkeypatch.setattr(store, "semantic_fingerprint", measured)
    monkeypatch.setattr(publisher, "_inspect_relations", inspected)
    published = publisher.publish(settings, conn)
    candidates = [(path, queries) for path, bound, queries in inspections if bound]
    assert len(candidates) == 1
    stage, queries = candidates[0]
    assert stage.name.startswith(".staging-")
    assert len(queries) == 9
    for table in SCHEMAS:
        assert queries.count(f"SELECT {projection(table)} FROM {table}") == 1
        assert sum(f"{stage / f'{table}.parquet'}" in query for query in queries) == 1
    assert sum("owner_venue" in query for query in queries) == 1
    # The only additional fingerprints inspect the previously published release.
    assert len(calls) == 13
    assert sum(str(previous) in query for query in calls) == 4

    release = Path(published["path"])
    for bound in (True, False, True, False):
        calls.clear()
        inspections.clear()
        assert publisher.verify_release(
            release, connection=conn if bound else None, settings=settings
        )["valid"]
        assert len(inspections) == 1
        assert inspections[0][:2] == (release, bound)
        assert len(calls) == (9 if bound else 4)
        assert sum(str(release) in query for query in calls) == 4


@pytest.mark.parametrize("api", ["release", "current", "publish"])
def test_fresh_call_detects_public_corruption_without_generation_change(catalogue, api):
    settings, conn, release = catalogue

    def verify():
        if api == "release":
            return publisher.verify_release(release, connection=conn)
        if api == "current":
            return publisher.verify_current(settings, connection=conn)
        return publisher.publish(settings, conn)

    assert verify()["valid"]
    generation = store.generation(conn)
    pointer = (settings.published_dir / "current.json").read_bytes()
    conn.execute("UPDATE outcomes SET outcome_label='' WHERE market_id='2' AND outcome_index=1")
    before = domain_snapshot(conn)
    with pytest.raises(CurrentError):
        verify()
    assert store.generation(conn) == generation
    assert domain_snapshot(conn) == before
    assert (settings.published_dir / "current.json").read_bytes() == pointer
    assert publisher.verify_current(settings)["valid"]


def assert_identity_gate_rejects_without_changing_publication(catalogue, original):
    settings, conn, release = catalogue
    damaged = domain_snapshot(conn)
    for table in (*SCHEMAS, "_identity_claims", "sync_state"):
        assert damaged[table] == original[table]
    pointer = (settings.published_dir / "current.json").read_bytes()
    with pytest.raises(CurrentError, match="current store integrity"):
        store.validate_store(conn)
    with pytest.raises(CurrentError, match="current store integrity"):
        publisher.verify_release(release, connection=conn)
    with pytest.raises(CurrentError, match="current store integrity"):
        publisher.publish(settings, conn)
    assert domain_snapshot(conn) == damaged
    assert (settings.published_dir / "current.json").read_bytes() == pointer
    assert publisher.verify_current(settings)["valid"]


def test_identity_existence_gate_accepts_healthy_usable_control(catalogue):
    settings, conn, release = catalogue
    before = domain_snapshot(conn)
    pointer = (settings.published_dir / "current.json").read_bytes()
    assert conn.execute("SELECT market_id,usable FROM markets ORDER BY market_id").fetchall() == [
        ("2", True),
        ("3", True),
    ]
    assert store.validate_store(conn)["valid"]
    assert publisher.verify_release(release, connection=conn)["valid"]
    assert publisher.publish(settings, conn)["noop"]
    assert domain_snapshot(conn) == before
    assert (settings.published_dir / "current.json").read_bytes() == pointer


@pytest.mark.parametrize("corruption", ["missing", "sql-null-accepted", "invalidated"])
def test_identity_existence_gate_rejects_unusable_identity_for_usable_market(catalogue, corruption):
    _, conn, _ = catalogue
    original = domain_snapshot(conn)
    if corruption == "missing":
        conn.execute("DELETE FROM _identity WHERE market_id='2'")
    elif corruption == "sql-null-accepted":
        conn.execute("UPDATE _identity SET accepted=NULL WHERE market_id='2'")
        assert conn.execute(
            "SELECT accepted IS NULL FROM _identity WHERE market_id='2'"
        ).fetchone() == (True,)
    else:
        conn.execute(
            "UPDATE _identity SET invalidated_at='2026-10-14T01:00:00Z' WHERE market_id='2'"
        )
    assert_identity_gate_rejects_without_changing_publication(catalogue, original)


@pytest.mark.parametrize("invalidated", [False, True], ids=["missing", "missing-and-invalid"])
def test_identity_existence_gate_rejects_bad_duplicate_despite_healthy_match(
    catalogue, invalidated
):
    _, conn, _ = catalogue
    original = domain_snapshot(conn)
    accepted_before = conn.execute(
        "SELECT venue,market_id,accepted FROM _identity ORDER BY ALL"
    ).fetchall()
    # A lost PK permits both a healthy row and an independently invalid match.
    # Looking only for a healthy match would silently accept this corruption.
    conn.execute("CREATE TABLE damaged_identity AS SELECT * FROM _identity")
    conn.execute("DROP TABLE _identity")
    conn.execute("ALTER TABLE damaged_identity RENAME TO _identity")
    conn.execute(
        "INSERT INTO _identity SELECT venue,market_id,candidate,NULL::JSON,"
        "CASE WHEN ? THEN '2026-10-14T01:00:00Z'::TIMESTAMPTZ END,"
        "CASE WHEN ? THEN 'invalidated-duplicate' END FROM _identity WHERE market_id='2'",
        [invalidated, invalidated],
    )
    assert conn.execute(
        "SELECT accepted IS NULL,invalidated_at IS NOT NULL FROM _identity "
        "WHERE market_id='2' ORDER BY ALL"
    ).fetchall() == [(False, False), (True, invalidated)]
    # The extra SQL NULL contributes no accepted outcomes: all healthy native
    # mappings and their multiplicities still agree with the public outcomes.
    assert (
        conn.execute(
            "SELECT venue,market_id,accepted FROM _identity WHERE accepted IS NOT NULL ORDER BY ALL"
        ).fetchall()
        == accepted_before
    )
    assert_identity_gate_rejects_without_changing_publication(catalogue, original)


@pytest.mark.parametrize(
    "corruption", ["swapped-owners", "wrong-venue", "null-venue", "wrong-market", "null-market"]
)
def test_accepted_native_rows_remain_bound_to_their_private_owner(catalogue, corruption):
    settings, conn, release = catalogue
    original = domain_snapshot(conn)
    maps = {
        mid: json.loads(accepted)
        for mid, accepted in conn.execute("SELECT market_id,accepted FROM _identity").fetchall()
    }
    if corruption == "swapped-owners":
        maps["2"], maps["3"] = maps["3"], maps["2"]
    else:
        key, value = {
            "wrong-venue": ("venue", "other-venue"),
            "null-venue": ("venue", None),
            "wrong-market": ("market_id", "3"),
            "null-market": ("market_id", None),
        }[corruption]
        for row in maps["2"]:
            row[key] = value
    for mid, rows in maps.items():
        conn.execute("UPDATE _identity SET accepted=? WHERE market_id=?", [json.dumps(rows), mid])
    if corruption == "swapped-owners":
        # A plain multiset fingerprint of the accepted public rows still agrees.
        # Only preserving their association with the private owner rejects this.
        def flatten(values):
            return sorted(json.dumps(row, sort_keys=True) for rows in values for row in rows)

        assert flatten(maps.values()) == flatten(
            json.loads(row[3]) for row in original["_identity"]
        )
    pointer = (settings.published_dir / "current.json").read_bytes()
    damaged = domain_snapshot(conn)
    for table in (*SCHEMAS, "_identity_claims", "sync_state"):
        assert damaged[table] == original[table]
    with pytest.raises(CurrentError, match="outcomes differ from accepted"):
        store.validate_store(conn)
    with pytest.raises(CurrentError, match="outcomes differ from accepted"):
        publisher.verify_release(release, connection=conn)
    with pytest.raises(CurrentError, match="outcomes differ from accepted"):
        publisher.publish(settings, conn)
    assert domain_snapshot(conn) == damaged
    assert (settings.published_dir / "current.json").read_bytes() == pointer
    # Independent public artifacts cannot attest to private accepted JSON.
    assert publisher.verify_release(release, settings=settings)["valid"]


@pytest.mark.parametrize("key", ["venue", "market_id"])
@pytest.mark.parametrize("missing", [False, True], ids=["foreign-owner", "null-owner"])
def test_independent_release_rejects_malformed_public_outcome_owner_keys(catalogue, key, missing):
    settings, conn, _ = catalogue
    pointer = (settings.published_dir / "current.json").read_bytes()
    expression = "NULL::VARCHAR" if missing else ("'other-venue'" if key == "venue" else "'999'")
    release, _ = rehashed_release(
        catalogue, {"outcomes": replace_column("outcomes", key, expression)}
    )
    with pytest.raises(CurrentError):
        publisher.verify_release(release, settings=settings)
    with pytest.raises(CurrentError):
        publisher.verify_release(release, connection=conn)
    assert (settings.published_dir / "current.json").read_bytes() == pointer
    assert store.validate_store(conn)["valid"]


@pytest.mark.parametrize("api", ["bound-release", "publication"])
def test_source_outcomes_are_compared_fresh_after_parquet_inspection(candidate, monkeypatch, api):
    settings, conn, previous, pointer, before = candidate
    fingerprint = publisher.semantic_fingerprint
    calls = []
    target = ".staging-" if api == "publication" else str(settings.root / "forged-release")

    def mutate_after_parquet(connection, query, parameters=None):
        result = fingerprint(connection, query, parameters)
        if target in query and "outcomes.parquet" in query and not calls:
            calls.append(query)
            connection.execute(
                "UPDATE outcomes SET outcome_label='Changed during verification' "
                "WHERE market_id='2' AND outcome_index=1"
            )
            assert store.generation(connection) == 2
        return result

    monkeypatch.setattr(publisher, "semantic_fingerprint", mutate_after_parquet)
    if api == "bound-release":
        # Bind to an exact export of the committed source before starting the hook.
        release, manifest = rehashed_release((settings, conn, previous))
        manifest["generation"] = store.generation(conn)
        (release / "manifest.json").write_text(json.dumps(manifest))
        # The direct API uses its caller's snapshot/transaction ownership.
        conn.execute("BEGIN")
        try:
            with pytest.raises(CurrentError, match="outcomes differ from accepted"):
                publisher.verify_release(release, connection=conn)
        finally:
            conn.execute("ROLLBACK")
    else:
        with pytest.raises(CurrentError, match="outcomes differ from accepted"):
            publisher.publish(settings, conn)
    assert len(calls) == 1
    assert domain_snapshot(conn) == before
    assert (settings.published_dir / "current.json").read_bytes() == pointer
    assert publisher.verify_current(settings)["valid"]


def test_private_core_returns_one_fresh_public_outcome_digest_without_public_api_leak(
    catalogue, monkeypatch
):
    _, conn, _ = catalogue
    source_query = f"SELECT {projection('outcomes')} FROM outcomes"
    golden = semantic_fingerprint(conn, source_query)
    calls = []

    def measured(connection, query, parameters=None):
        result = semantic_fingerprint(connection, query, parameters)
        calls.append((query, result))
        return result

    monkeypatch.setattr(store, "semantic_fingerprint", measured)
    expected_report = {"valid": True, "generation": store.generation(conn)}
    report, fresh_source = store._validate_store(conn)
    assert report == expected_report
    assert fresh_source == golden
    assert len(calls) == 2
    assert [result for query, result in calls if query == source_query] == [golden]
    assert all(result == golden for _, result in calls)
    calls.clear()
    assert store.validate_store(conn) == expected_report
    assert len(calls) == 2
    assert [result for query, result in calls if query == source_query] == [golden]
