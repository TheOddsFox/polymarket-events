"""Native semantics survive validation even when hashes, counts and claims look valid."""

import json
from pathlib import Path

import duckdb
import pytest

from oddsfox_catalogue import current_publish as publisher
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import SCHEMAS, CurrentError, projection
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.fingerprints import semantic_fingerprint

UINT256_MAX = str(2**256 - 1)
UINT256_OVERFLOW = str(2**256)
SEMANTIC_CORRUPTIONS = (
    "zero-index",
    "gapped-index",
    "selected-alias",
    "asset-kind",
    "parent-condition",
    "protocol-version",
    "null-protocol",
    "null-version",
    "unknown-version",
    "null-label",
    "empty-label",
    "duplicate-asset",
    "partial-counterpart",
    "duplicate-counterpart",
    "noncanonical-counterpart",
    "overflow-counterpart",
    "zero-native",
    "overflow-native",
    "noncanonical-native",
)


@pytest.fixture
def catalogue(tmp_path):
    settings = CurrentSettings(tmp_path / "catalogue")
    validate_root(settings.root, initialize=True)
    with duckdb.connect(str(settings.store_path)) as conn:
        store.initialize(conn)
        yield settings, conn


def market(version, *, null_condition=False, counterpart=True):
    raw = {
        "id": "2",
        "version": version,
        "conditionId": None if null_condition else "0x" + "a" * 64,
        "outcomes": ["Yes", "No"],
        "clobTokenIds": ["11", "12"],
        "positionIds": ["21", "22"],
    }
    if not counterpart:
        raw.pop("positionIds" if version == "v1" else "clobTokenIds")
    return raw


def apply(conn, raw):
    rows = project_response(
        raw,
        "markets",
        received_at="2026-10-14T00:00:00Z",
        response_id="native",
        native_market_ids=["2"],
    )
    store.apply(conn, rows)
    return rows


def publish_valid(settings, conn, version, **kwargs):
    rows = apply(conn, market(version, **kwargs))
    assert rows.markets[0]["usable"]
    assert store.validate_store(conn)["valid"]
    result = publisher.publish(settings, conn)
    assert publisher.verify_release(result["path"], settings=settings)["valid"]
    return Path(result["path"])


def corrupt(conn, version, case):
    selected = "clob_token_id" if version == "v1" else "position_id"
    counterpart = "position_id" if version == "v1" else "clob_token_id"
    other_kind = "poly_v2_position" if version == "v1" else "ctf_token"
    other_version = "v2" if version == "v1" else "v1"
    if case == "duplicate-asset":
        first = "11" if version == "v1" else "21"
        conn.execute(
            f"UPDATE outcomes SET asset_id=?,{selected}=? WHERE outcome_index=2", [first, first]
        )
    elif case == "swap-assets":
        first, second = ("11", "12") if version == "v1" else ("21", "22")
        conn.execute(
            f"UPDATE outcomes SET asset_id=CASE outcome_index WHEN 1 THEN ? ELSE ? END,{selected}=CASE outcome_index WHEN 1 THEN ? ELSE ? END",
            [second, first, second, first],
        )
    elif case == "swap-labels":
        conn.execute(
            "UPDATE outcomes SET outcome_label=CASE outcome_index WHEN 1 THEN 'No' ELSE 'Yes' END"
        )
    else:
        query = {
            "zero-index": "UPDATE outcomes SET outcome_index=0 WHERE outcome_index=1",
            "gapped-index": "UPDATE outcomes SET outcome_index=3 WHERE outcome_index=2",
            "selected-alias": f"UPDATE outcomes SET {selected}='99' WHERE outcome_index=1",
            "asset-kind": f"UPDATE outcomes SET asset_kind='{other_kind}'",
            "parent-condition": "UPDATE markets SET condition_id='0x" + "b" * 64 + "'",
            "protocol-version": f"UPDATE markets SET source_market_version='{other_version}'",
            "null-protocol": "UPDATE markets SET protocol=NULL",
            "null-version": "UPDATE markets SET source_market_version=NULL",
            "unknown-version": "UPDATE markets SET source_market_version='v3'",
            "null-label": "UPDATE outcomes SET outcome_label=NULL WHERE outcome_index=1",
            "empty-label": "UPDATE outcomes SET outcome_label='' WHERE outcome_index=1",
            "partial-counterpart": f"UPDATE outcomes SET {counterpart}=NULL WHERE outcome_index=1",
            "duplicate-counterpart": f"UPDATE outcomes SET {counterpart}=(SELECT {counterpart} FROM outcomes WHERE outcome_index=1) WHERE outcome_index=2",
            "noncanonical-counterpart": f"UPDATE outcomes SET {counterpart}='01' WHERE outcome_index=1",
            "overflow-counterpart": f"UPDATE outcomes SET {counterpart}='{UINT256_OVERFLOW}' WHERE outcome_index=1",
            "zero-native": f"UPDATE outcomes SET asset_id='0',{selected}='0' WHERE outcome_index=1",
            "overflow-native": f"UPDATE outcomes SET asset_id='{UINT256_OVERFLOW}',{selected}='{UINT256_OVERFLOW}' WHERE outcome_index=1",
            "noncanonical-native": f"UPDATE outcomes SET asset_id='01',{selected}='01' WHERE outcome_index=1",
        }[case]
        conn.execute(query)


@pytest.mark.parametrize("version", ["v1", "v2"])
@pytest.mark.parametrize("case", [*SEMANTIC_CORRUPTIONS, "swap-assets", "swap-labels"])
def test_private_native_corruption_cannot_validate_or_publish(catalogue, version, case):
    settings, conn = catalogue
    publish_valid(settings, conn, version)
    pointer = settings.published_dir / "current.json"
    before = pointer.read_bytes()
    claims = conn.execute("SELECT * FROM _identity_claims ORDER BY ALL").fetchall()
    corrupt(conn, version, case)
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone() == (2,)
    assert conn.execute("SELECT * FROM _identity_claims ORDER BY ALL").fetchall() == claims
    assert conn.execute("SELECT json_array_length(accepted) FROM _identity").fetchone() == (2,)
    with pytest.raises(CurrentError):
        store.validate_store(conn)
    with pytest.raises(CurrentError):
        publisher.publish(settings, conn)
    assert pointer.read_bytes() == before
    assert publisher.verify_current(settings)["valid"]


@pytest.mark.parametrize("version", ["v1", "v2"])
@pytest.mark.parametrize("case", SEMANTIC_CORRUPTIONS)
def test_independent_release_rejects_semantic_corruption_after_manifest_rehash(
    catalogue, version, case
):
    settings, conn = catalogue
    release = publish_valid(settings, conn, version)
    corrupt(conn, version, case)
    manifest_path = release / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for table in ("markets", "outcomes"):
        file = release / f"{table}.parquet"
        publisher._export(conn, table, file)
        manifest["relations"][table] = {
            **semantic_fingerprint(conn, f"SELECT {projection(table)} FROM {table}"),
            **publisher._physical(file),
        }
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(CurrentError):
        publisher.verify_release(release, settings=settings)


def test_accepted_maps_swapped_between_owners_cannot_validate_or_publish(catalogue):
    settings, conn = catalogue
    second = {
        **market("v1"),
        "id": "3",
        "conditionId": "0x" + "b" * 64,
        "clobTokenIds": ["31", "32"],
        "positionIds": ["41", "42"],
    }
    store.apply(
        conn,
        project_response(
            [market("v1"), second],
            "markets",
            received_at="2026-10-14T00:00:00Z",
            response_id="two-native-owners",
            native_market_ids=["2", "3"],
        ),
    )
    assert store.validate_store(conn)["valid"]
    publisher.publish(settings, conn)
    pointer = settings.published_dir / "current.json"
    before = pointer.read_bytes()
    claims = conn.execute("SELECT * FROM _identity_claims ORDER BY ALL").fetchall()
    outcomes = conn.execute("SELECT * FROM outcomes ORDER BY ALL").fetchall()
    accepted = conn.execute("SELECT accepted FROM _identity ORDER BY market_id").fetchall()
    conn.execute(
        "UPDATE _identity SET accepted=CASE market_id WHEN '2' THEN ? ELSE ? END",
        [accepted[1][0], accepted[0][0]],
    )
    assert conn.execute(
        "SELECT market_id,json_extract_string(accepted,'$[0].market_id'),json_array_length(accepted) FROM _identity ORDER BY market_id"
    ).fetchall() == [("2", "3", 2), ("3", "2", 2)]
    assert conn.execute("SELECT count(*) FROM markets WHERE usable").fetchone() == (2,)
    assert conn.execute("SELECT * FROM _identity_claims ORDER BY ALL").fetchall() == claims
    assert conn.execute("SELECT * FROM outcomes ORDER BY ALL").fetchall() == outcomes
    with pytest.raises(CurrentError):
        store.validate_store(conn)
    with pytest.raises(CurrentError):
        publisher.publish(settings, conn)
    assert pointer.read_bytes() == before
    assert publisher.verify_current(settings)["valid"]


@pytest.mark.parametrize("mutation", ["truncate", "duplicate", "empty"])
def test_accepted_map_cardinality_corruption_cannot_validate_or_publish(catalogue, mutation):
    settings, conn = catalogue
    publish_valid(settings, conn, "v1")
    pointer = settings.published_dir / "current.json"
    before = pointer.read_bytes()
    outcomes = conn.execute("SELECT * FROM outcomes ORDER BY ALL").fetchall()
    claims = conn.execute("SELECT * FROM _identity_claims ORDER BY ALL").fetchall()
    accepted = json.loads(conn.execute("SELECT accepted FROM _identity").fetchone()[0])
    corrupted = {
        "truncate": accepted[:1],
        "duplicate": accepted + [accepted[0]],
        "empty": [],
    }[mutation]
    conn.execute("UPDATE _identity SET accepted=?", [json.dumps(corrupted)])
    assert conn.execute("SELECT json_array_length(accepted) FROM _identity").fetchone() == (
        len(corrupted),
    )
    assert len(outcomes) == 2
    assert conn.execute("SELECT * FROM outcomes ORDER BY ALL").fetchall() == outcomes
    assert conn.execute("SELECT * FROM _identity_claims ORDER BY ALL").fetchall() == claims
    with pytest.raises(CurrentError):
        store.validate_store(conn)
    with pytest.raises(CurrentError):
        publisher.publish(settings, conn)
    assert pointer.read_bytes() == before
    assert publisher.verify_current(settings)["valid"]


@pytest.mark.parametrize("version,null_condition", [("v1", False), ("v2", False), ("v2", True)])
@pytest.mark.parametrize("counterpart", [False, True])
def test_valid_native_maps_keep_optional_unequal_aliases_and_v2_null_condition(
    catalogue, version, null_condition, counterpart
):
    settings, conn = catalogue
    release = publish_valid(
        settings, conn, version, null_condition=null_condition, counterpart=counterpart
    )
    expected = ["11", "12"] if version == "v1" else ["21", "22"]
    assert conn.execute(
        "SELECT outcome_index,asset_id FROM outcomes ORDER BY outcome_index"
    ).fetchall() == [(1, expected[0]), (2, expected[1])]
    assert publisher.verify_release(release, settings=settings)["valid"]
    if null_condition:
        assert conn.execute("SELECT condition_id FROM markets").fetchone() == (None,)
        assert conn.execute("SELECT condition_id FROM outcomes").fetchall() == [(None,), (None,)]
    if counterpart:
        assert conn.execute(
            "SELECT count(*) FROM outcomes WHERE clob_token_id<>position_id"
        ).fetchone() == (2,)


@pytest.mark.parametrize("version", ["v1", "v2"])
def test_uint256_max_remains_exact_in_private_store_and_independent_release(catalogue, version):
    settings, conn = catalogue
    raw = market(version)
    raw["clobTokenIds" if version == "v1" else "positionIds"][1] = UINT256_MAX
    rows = apply(conn, raw)
    assert rows.markets[0]["usable"]
    assert conn.execute("SELECT asset_id FROM outcomes WHERE outcome_index=2").fetchone() == (
        UINT256_MAX,
    )
    assert store.validate_store(conn)["valid"]
    result = publisher.publish(settings, conn)
    assert publisher.verify_release(result["path"], settings=settings)["valid"]


@pytest.mark.parametrize("version", ["v1", "v2"])
def test_uint256_max_plus_one_is_retained_as_unusable_without_assets(catalogue, version):
    settings, conn = catalogue
    raw = market(version)
    raw["clobTokenIds" if version == "v1" else "positionIds"][1] = UINT256_OVERFLOW
    rows = apply(conn, raw)
    assert rows.markets[0]["usable"] is False
    assert rows.outcomes == []
    assert conn.execute("SELECT usable FROM markets").fetchone() == (False,)
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone() == (0,)
    assert store.validate_store(conn)["valid"]
    assert publisher.publish(settings, conn)["valid"]


@pytest.mark.parametrize(
    "version,field,json_encoded",
    [
        ("v1", "clobTokenIds", False),
        ("v1", "positionIds", True),
        ("v2", "positionIds", True),
        ("v2", "clobTokenIds", False),
    ],
)
def test_oversized_native_id_quarantines_only_its_market(catalogue, version, field, json_encoded):
    settings, conn = catalogue
    invalid = market(version)
    invalid[field][1] = "9" * 5000
    if json_encoded:
        invalid[field] = json.dumps(invalid[field])
    valid = {
        **market(version),
        "id": "3",
        "conditionId": "0x" + "b" * 64,
        "clobTokenIds": ["31", "32"],
        "positionIds": ["41", "42"],
    }
    rows = project_response(
        [invalid, valid],
        "markets",
        received_at="2026-10-14T00:00:00Z",
        response_id="oversized-native-id",
        native_market_ids=["2", "3"],
    )
    assert [(row["market_id"], row["usable"]) for row in rows.markets] == [
        ("2", False),
        ("3", True),
    ]
    assert field in rows.markets[0]["identity_error"]
    expected = ["31", "32"] if version == "v1" else ["41", "42"]
    assert [(row["market_id"], row["asset_id"]) for row in rows.outcomes] == [
        ("3", asset_id) for asset_id in expected
    ]
    store.apply(conn, rows)
    assert conn.execute("SELECT market_id,usable FROM markets ORDER BY market_id").fetchall() == [
        ("2", False),
        ("3", True),
    ]
    assert conn.execute("SELECT DISTINCT market_id FROM outcomes").fetchall() == [("3",)]
    assert store.validate_store(conn)["valid"]
    release = publisher.publish(settings, conn)
    assert publisher.verify_release(release["path"], settings=settings)["valid"]


def test_long_leading_zero_native_id_is_quarantined_as_noncanonical():
    raw = market("v1")
    raw["clobTokenIds"][0] = "0" * 5000 + "1"
    rows = project_response(
        raw,
        "markets",
        received_at="2026-10-14T00:00:00Z",
        response_id="long-leading-zero-native-id",
    )
    assert rows.markets[0]["usable"] is False
    assert rows.markets[0]["identity_error"] == "clobTokenIds contains a noncanonical native ID"
    assert rows.outcomes == []


@pytest.mark.parametrize("scenario", ["v1-counterpart", "v2-counterpart", "different-asset-kinds"])
def test_native_uniqueness_respects_optional_counterparts_and_selected_namespaces(
    catalogue, scenario
):
    settings, conn = catalogue
    version = "v2" if scenario == "v2-counterpart" else "v1"
    first = market(version)
    second = {**market(version), "id": "3", "conditionId": "0x" + "b" * 64}
    if scenario == "different-asset-kinds":
        second.update(
            version="v2", conditionId=None, clobTokenIds=["31", "32"], positionIds=["11", "12"]
        )
    else:
        second["clobTokenIds" if version == "v1" else "positionIds"] = ["31", "32"]
    store.apply(
        conn,
        project_response(
            [first, second],
            "markets",
            received_at="2026-10-14T00:00:00Z",
            response_id="valid-namespaces",
            native_market_ids=["2", "3"],
        ),
    )
    assert conn.execute("SELECT market_id,usable FROM markets ORDER BY market_id").fetchall() == [
        ("2", True),
        ("3", True),
    ]
    assert conn.execute("SELECT count(*) FROM outcomes").fetchone() == (4,)
    if scenario == "different-asset-kinds":
        assert conn.execute(
            "SELECT asset_id,count(*),count(DISTINCT asset_kind) FROM outcomes GROUP BY asset_id ORDER BY asset_id"
        ).fetchall() == [("11", 2, 2), ("12", 2, 2)]
    else:
        counterpart = "position_id" if version == "v1" else "clob_token_id"
        expected = ["21", "22"] if version == "v1" else ["11", "12"]
        assert conn.execute(
            f"SELECT {counterpart},count(DISTINCT market_id) FROM outcomes GROUP BY {counterpart} ORDER BY {counterpart}"
        ).fetchall() == [(value, 2) for value in expected]
    assert store.validate_store(conn)["valid"]
    release = publisher.publish(settings, conn)
    assert publisher.verify_release(release["path"], settings=settings)["valid"]


@pytest.mark.parametrize("version", ["v1", "v2"])
def test_duplicate_selected_asset_across_markets_in_one_namespace_still_fails(catalogue, version):
    settings, conn = catalogue
    selected = "clob_token_id" if version == "v1" else "position_id"
    second = {**market(version), "id": "3", "conditionId": "0x" + "b" * 64}
    second["clobTokenIds" if version == "v1" else "positionIds"] = ["31", "32"]
    store.apply(
        conn,
        project_response(
            [market(version), second],
            "markets",
            received_at="2026-10-14T00:00:00Z",
            response_id="distinct-selected-assets",
            native_market_ids=["2", "3"],
        ),
    )
    assert store.validate_store(conn)["valid"]
    publisher.publish(settings, conn)
    pointer = settings.published_dir / "current.json"
    before = pointer.read_bytes()
    duplicate = "11" if version == "v1" else "21"
    conn.execute(
        f"UPDATE outcomes SET asset_id=?,{selected}=? WHERE market_id='3' AND outcome_index=1",
        [duplicate, duplicate],
    )
    assert conn.execute(
        "SELECT count(*) FROM outcomes WHERE asset_id=?", [duplicate]
    ).fetchone() == (2,)
    with pytest.raises(CurrentError):
        publisher._integrity(conn, {table: table for table in SCHEMAS})
    with pytest.raises(CurrentError):
        store.validate_store(conn)
    with pytest.raises(CurrentError):
        publisher.publish(settings, conn)
    assert pointer.read_bytes() == before
    assert publisher.verify_current(settings)["valid"]


@pytest.mark.parametrize("version", ["v1", "v2"])
def test_candidate_claim_cannot_substitute_for_missing_accepted_claim(catalogue, version):
    settings, conn = catalogue
    publish_valid(settings, conn, version)
    pointer = settings.published_dir / "current.json"
    before = pointer.read_bytes()
    first = "11" if version == "v1" else "21"
    outcomes = conn.execute("SELECT * FROM outcomes ORDER BY ALL").fetchall()
    conn.execute(
        "DELETE FROM _identity_claims WHERE market_id='2' AND role='accepted' AND asset_id=?",
        [first],
    )
    assert conn.execute(
        "SELECT role,count(*) FROM _identity_claims WHERE market_id='2' AND asset_id=? GROUP BY role",
        [first],
    ).fetchall() == [("candidate", 1)]
    assert conn.execute("SELECT * FROM outcomes ORDER BY ALL").fetchall() == outcomes
    publisher._integrity(conn, {table: table for table in SCHEMAS})
    with pytest.raises(CurrentError):
        store.validate_store(conn)
    with pytest.raises(CurrentError):
        publisher.publish(settings, conn)
    assert pointer.read_bytes() == before
    assert publisher.verify_current(settings)["valid"]


@pytest.mark.parametrize("reservation", ["accepted", "candidate"])
def test_unusable_owner_reservations_block_usable_owner_until_both_are_quarantined(
    catalogue, reservation
):
    settings, conn = catalogue
    hidden = {
        **market("v1"),
        "id": "3",
        "conditionId": "0x" + "b" * 64,
        "clobTokenIds": ["31", "32"],
        "positionIds": ["41", "42"],
    }
    store.apply(
        conn,
        project_response(
            [market("v1"), hidden],
            "markets",
            received_at="2026-10-14T00:00:00Z",
            response_id="distinct-reservations",
            native_market_ids=["2", "3"],
        ),
    )
    assert store.validate_store(conn)["valid"]
    publisher.publish(settings, conn)
    pointer = settings.published_dir / "current.json"
    before = pointer.read_bytes()
    reserved = project_response(
        {**hidden, "clobTokenIds": ["11", "12"]},
        "markets",
        received_at="2026-10-15T00:00:00Z",
        response_id="hidden-reservation",
    )
    conn.execute(
        f"UPDATE _identity SET {reservation}=?,invalidated_at='2026-10-15T00:00:00Z',invalidated_response='hidden-reservation' WHERE market_id='3'",
        [json.dumps(reserved.outcomes)],
    )
    conn.execute(
        "UPDATE _identity_claims SET asset_id=CASE asset_id WHEN '31' THEN '11' ELSE '12' END WHERE market_id='3' AND role=?",
        [reservation],
    )
    conn.execute(
        "UPDATE markets SET usable=false,identity_error='native_confirmation_required' WHERE market_id='3'"
    )
    conn.execute("DELETE FROM outcomes WHERE market_id='3'")
    assert conn.execute("SELECT market_id,usable FROM markets ORDER BY market_id").fetchall() == [
        ("2", True),
        ("3", False),
    ]
    assert conn.execute(
        "SELECT role,asset_id FROM _identity_claims WHERE market_id='3' AND asset_id IN ('11','12') ORDER BY asset_id"
    ).fetchall() == [(reservation, "11"), (reservation, "12")]
    publisher._integrity(conn, {table: table for table in SCHEMAS})
    with pytest.raises(CurrentError):
        store.validate_store(conn)
    with pytest.raises(CurrentError):
        publisher.publish(settings, conn)
    assert pointer.read_bytes() == before
    assert publisher.verify_current(settings)["valid"]
    claims = conn.execute("SELECT * FROM _identity_claims ORDER BY ALL").fetchall()
    conn.execute("UPDATE markets SET usable=false,identity_error='asset_ownership_conflict'")
    conn.execute(
        "UPDATE _identity SET invalidated_at='2026-10-15T00:00:00Z',invalidated_response='quarantine'"
    )
    conn.execute("DELETE FROM outcomes")
    store.set_state(conn, "generation", store.generation(conn) + 1)
    assert conn.execute("SELECT * FROM _identity_claims ORDER BY ALL").fetchall() == claims
    assert store.validate_store(conn)["valid"]
    release = publisher.publish(settings, conn)
    assert release["counts"]["markets"] == 2
    assert release["counts"]["outcomes"] == 0
    assert publisher.verify_release(release["path"], settings=settings)["valid"]
