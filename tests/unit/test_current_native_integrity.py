"""Native semantics survive validation even when hashes, counts and claims look valid."""

import json
from pathlib import Path

import duckdb
import pytest

from oddsfox_catalogue import current_publish as publisher
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import CurrentError, projection
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
