#!/usr/bin/env python3
"""Bounded, synthetic-delta v3 benchmark. Never accesses live Gamma.

Requires a verified, idle v3 seed. Copies only its stamp, store, publication and
configuration, serially; retains failed copies and removes verified successful
copies. This measures normal refresh, not six-hour/monthly reconciliation.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import resource
import shutil
import subprocess
import sys
import time
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import duckdb

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tests"))
from fakes.current_gamma import CurrentGammaServer  # noqa: E402
from oddsfox_catalogue.current_config import (  # noqa: E402
    CurrentSettings,
    connect_store,
    safe_path,
    validate_root,
)
from oddsfox_catalogue.current_contract import SCHEMAS, columns  # noqa: E402
from oddsfox_catalogue.current_publish import verify_current  # noqa: E402
from oddsfox_catalogue.current_store import get_state, validate_store  # noqa: E402
from oddsfox_catalogue.limits import directory_bytes  # noqa: E402


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024**2):
            result.update(block)
    return result.hexdigest()


def records(conn, table, where="true", params=()):
    result = conn.execute(f"SELECT * FROM {table} WHERE {where}", params)
    names = [column[0] for column in result.description]
    return [dict(zip(names, row, strict=True)) for row in result.fetchall()]


def market_payload(conn, row):
    outcomes = records(conn, "outcomes", "market_id=? ORDER BY outcome_index", [row["market_id"]])
    require(row["usable"] and outcomes, "selected old membership requires usable outcomes")
    return {
        "id": row["market_id"],
        "question": row["question"],
        "slug": row["slug"],
        "conditionId": row["condition_id"],
        "version": row["source_market_version"],
        **{key: row[key] for key in ("active", "closed", "archived")},
        "outcomes": [o["outcome_label"] for o in outcomes],
        "clobTokenIds": [o["clob_token_id"] for o in outcomes]
        if all(o["clob_token_id"] is not None for o in outcomes)
        else None,
        "positionIds": [o["position_id"] for o in outcomes]
        if all(o["position_id"] is not None for o in outcomes)
        else None,
    }


def retained_maxima(conn):
    return {
        lane: conn.execute(
            f"SELECT coalesce(max(CAST({lane[:-1]}_id AS HUGEINT)), 0) "
            f"FROM {lane} WHERE venue='polymarket'"
        ).fetchone()[0]
        for lane in ("events", "markets")
    }


def build_source(conn, frontiers, count):
    """Sample retained fields; create exact disjoint native IDs independently."""
    require(count >= 12, "delta must contain at least 12 markets")
    require(
        all(
            type(frontiers.get(lane)) is int and 0 <= frontiers[lane] < 10**20
            for lane in ("events", "markets")
        ),
        "seed frontiers must be canonical nonnegative integers of at most 20 digits",
    )
    maxima = retained_maxima(conn)
    bases = {lane: max(frontiers[lane], maxima[lane]) for lane in frontiers}
    require(
        all(bases[lane] + count + math.ceil(count / 4) + 1 < 10**20 for lane in bases),
        "synthetic entities and absent controls exceed canonical 20-digit IDs",
    )
    samples = records(conn, "markets", "true ORDER BY CAST(market_id AS HUGEINT) LIMIT 1000")
    require(samples, "seed has no market field samples")
    selected = records(
        conn,
        "events",
        "event_id IN (SELECT e.event_id FROM events e "
        "WHERE NOT EXISTS (SELECT 1 FROM event_markets l JOIN markets m USING(venue,market_id) "
        "WHERE l.event_id=e.event_id AND NOT m.usable) "
        "AND (SELECT count(*) FROM event_markets l WHERE l.event_id=e.event_id) <= 100 "
        "ORDER BY CAST(e.event_id AS HUGEINT) LIMIT 10) ORDER BY CAST(event_id AS HUGEINT)",
    )
    require(len(selected) == 10, "seed needs ten events with reconstructable memberships")
    events, markets = {}, {}
    for row in selected:
        event = {key: row[key] for key in ("title", "slug", "active", "closed", "archived")}
        event.update(id=row["event_id"], markets=[])
        for market in records(
            conn,
            "markets",
            "market_id IN (SELECT market_id FROM event_markets WHERE event_id=?)",
            [row["event_id"]],
        ):
            payload = market_payload(conn, market)
            markets[payload["id"]] = payload
            event["markets"].append(payload)
        events[event["id"]] = event
    n_events = math.ceil(count / 4)
    new_events = []
    for index in range(n_events):
        template = selected[index % len(selected)]
        eid = str(bases["events"] + index + 1)
        event = {key: template[key] for key in ("title", "slug", "archived")}
        event.update(
            id=eid,
            active=(True, False, None)[index % 3],
            closed=(False, True, None)[index % 3],
            markets=[],
        )
        events[eid] = event
        new_events.append(eid)
    new_markets = []
    for index in range(count):
        sample = samples[index % len(samples)]
        mid = str(bases["markets"] + index + 1)
        eid = selected[index]["event_id"] if index < 10 else new_events[index % n_events]
        version = sample["source_market_version"]
        version = version if version and version.lower() in {"v1", "v2"} else "v2"
        outcome_count = (8, 16, 32)[index] if index < 3 else 2
        if count >= 1000 and 10 <= index < 130:
            eid = new_events[0]
        row = {
            "id": mid,
            "question": sample["question"],
            "slug": sample["slug"],
            "conditionId": "0x" + f"{int(mid):064x}",
            "version": version,
            "active": (True, False, None)[index % 3],
            "closed": (False, True, None)[index % 3],
            "archived": sample["archived"],
            "outcomes": [f"Outcome {i + 1}" for i in range(outcome_count)]
            if outcome_count > 2
            else ["Yes", "No"],
            "clobTokenIds": None,
            "positionIds": None,
            "events": [{"id": eid}],
        }
        markets[mid] = row
        events[eid]["markets"].append(row)
        new_markets.append(mid)
    # Hash over the full uint256 domain so lookup cannot prune all seed row groups.
    slots = [(mid, i) for mid in new_markets for i in range(len(markets[mid]["outcomes"]))]
    assets, attempts = {}, dict.fromkeys(slots, 0)
    for _ in range(16):
        reserved = set(assets.values())
        for slot in slots:
            if slot in assets:
                continue
            mid, outcome = slot
            value = int.from_bytes(
                hashlib.sha256(
                    f"benchmark-current-v1:{mid}:{outcome}:{attempts[slot]}".encode()
                ).digest(),
                "big",
            )
            attempts[slot] += 1
            if value and str(value) not in reserved:
                assets[slot] = str(value)
                reserved.add(str(value))
        # One bounded candidate list checks accepted AND pending ownership reservations.
        conflicts = {
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT c.asset_id FROM _identity_claims c "
                "JOIN unnest(?::VARCHAR[]) proposed(asset_id) USING(asset_id) "
                "WHERE c.venue='polymarket'",
                [list(assets.values())],
            ).fetchall()
        }
        assets = {slot: asset for slot, asset in assets.items() if asset not in conflicts}
        if len(assets) == len(slots):
            break
    require(len(assets) == len(slots), "could not allocate disjoint native uint256 identifiers")
    for mid in new_markets:
        market = markets[mid]
        key = "positionIds" if market["version"].lower() == "v2" else "clobTokenIds"
        market[key] = [assets[mid, i] for i in range(len(market["outcomes"]))]
    controls = {}
    for lane, source in (("event", events), ("market", markets)):
        for status, flag in (("open", False), ("closed", True)):
            controls[f"{status}-{lane}"] = next(
                key for key, row in source.items() if row["closed"] is flag
            )
        controls[f"absent-{lane}"] = str(bases[lane + "s"] + count + n_events + 1)
    return events, markets, controls, new_events, new_markets


def expected_delta(events, markets, new_events, new_markets):
    """Independent expected projection; does not call the production projector."""
    result = {table: [] for table in SCHEMAS}
    for eid in new_events:
        event = events[eid]
        result["events"].append(
            {
                "venue": "polymarket",
                "event_id": eid,
                **{key: event[key] for key in ("title", "slug", "active", "closed", "archived")},
            }
        )
    for mid in new_markets:
        m = markets[mid]
        v2 = m["version"].lower() == "v2"
        result["markets"].append(
            dict(
                venue="polymarket",
                market_id=mid,
                question=m["question"],
                slug=m["slug"],
                condition_id=m["conditionId"],
                source_market_version=m["version"],
                protocol="polymarket_v2" if v2 else "ctf",
                active=m["active"],
                closed=m["closed"],
                archived=m["archived"],
                usable=True,
                identity_error=None,
            )
        )
        for index, label in enumerate(m["outcomes"]):
            token = m["positionIds" if v2 else "clobTokenIds"][index]
            result["outcomes"].append(
                dict(
                    venue="polymarket",
                    market_id=mid,
                    condition_id=m["conditionId"],
                    outcome_index=index + 1,
                    outcome_label=label,
                    asset_kind="poly_v2_position" if v2 else "ctf_token",
                    asset_id=token,
                    clob_token_id=None if v2 else token,
                    position_id=token if v2 else None,
                )
            )
        result["event_markets"].append(
            dict(venue="polymarket", event_id=m["events"][0]["id"], market_id=mid)
        )
    return result


def verify_expected(conn, seed_release, delta):
    counts = {}
    for table, rows in delta.items():
        conn.execute(
            f"CREATE TEMP TABLE expected_{table} AS SELECT * FROM read_parquet(?)",
            [str(seed_release / f"{table}.parquet")],
        )
        names = columns(table)
        if rows:
            conn.executemany(
                f"INSERT INTO expected_{table} VALUES ({','.join('?' for _ in names)})",
                [[r[n] for n in names] for r in rows],
            )
        mismatch = conn.execute(
            f"SELECT * FROM ((SELECT * FROM expected_{table} EXCEPT ALL SELECT * FROM {table}) UNION ALL (SELECT * FROM {table} EXCEPT ALL SELECT * FROM expected_{table})) LIMIT 1"
        ).fetchone()
        require(mismatch is None, f"exact seed-plus-delta mismatch: {table}: {mismatch}")
        counts[table] = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    return counts


def cli(root, source, executable, *args):
    env = {key: value for key, value in os.environ.items() if not key.startswith("CATALOGUE_")}
    env.update(CATALOGUE_GAMMA_BASE_URL=source, CATALOGUE_GAMMA_ALLOW_LOOPBACK="1")
    # Pin every accepted ceiling instead of inheriting operator configuration.
    defaults = CurrentSettings(root)
    for section, values in (
        ("CATALOGUE", asdict(defaults.limits)),
        ("GAMMA", asdict(defaults.gamma)),
    ):
        for key, value in values.items():
            if key not in {"base_url", "allow_loopback"}:
                env[f"CATALOGUE_{section}_{key.upper()}"] = (
                    str(value).lower() if isinstance(value, bool) else str(value)
                )
    command = [executable] if executable else [sys.executable, "-m", "oddsfox_catalogue"]
    started = time.monotonic()
    completed = subprocess.run(
        [*command, "--root", str(root), *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=310,
        check=False,
    )
    peak = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    result = {
        "process_rss": {
            "children_cumulative_peak_bytes": peak if sys.platform == "darwin" else peak * 1024,
            "scope": "all completed CLI children so far; not isolated per invocation",
        },
        "command": command + ["--root", str(root), *args],
        "elapsed_wall_s": time.monotonic() - started,
        "returncode": completed.returncode,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
    }
    if completed.returncode == 0:
        result["result"] = json.loads(completed.stdout)
    return result


def successful(invocation):
    require(invocation["returncode"] == 0, invocation["stderr"] + invocation["stdout"])
    return invocation["result"]


def verify_highwater_only(responses, certificate):
    expected = Counter()
    for lane, policy in certificate["lanes"].items():
        passes = policy.get("passes")
        require(passes == [None] or passes == [False, True], "unexpected certified passes")
        expected.update((lane, closed) for closed in passes)
    actual = Counter()
    for response in responses:
        parsed = urlsplit(response["path"])
        query = parse_qs(parsed.query)
        closed = query.pop("closed", None)
        require(closed in (None, ["false"], ["true"]), "noncanonical highwater closed pass")
        require(
            response["status"] == 200
            and query == {"limit": ["1"], "order": ["id"], "ascending": ["false"]},
            "unchanged refresh did more than certified highwater requests",
        )
        actual[(parsed.path.strip("/"), None if closed is None else closed == ["true"])] += 1
    require(actual == expected, "unchanged refresh highwater passes differ from certificate")


def gap_discovery(responses, frontiers, retained, elapsed):
    attempts = dict.fromkeys(frontiers, 0)
    for response in responses:
        parsed = urlsplit(response["path"])
        lane = parsed.path.strip("/")
        ids = parse_qs(parsed.query).get("id", [])
        if lane in frontiers and any(
            frontiers[lane] < int(entity_id) <= retained[lane] for entity_id in ids
        ):
            attempts[lane] += 1
    return {
        "window_http_attempts": attempts,
        "included_in_refresh_elapsed_wall_s": elapsed,
        "timing_scope": "whole refresh including gap windows; gap time is not isolated",
    }


def accounting_roots(paths):
    """Count overlapping directory trees once, with ancestors taking precedence."""
    roots = []
    for path in sorted({safe_path(Path(p)) for p in paths}, key=lambda p: len(p.parts)):
        if not any(root == path or root in path.parents for root in roots):
            roots.append(path)
    return roots


def capacity(seed, output, limit, additional=0, *, account_roots=()):
    roots = accounting_roots([seed, output, *account_roots])
    sizes = {str(path): directory_bytes(path) for path in roots}
    retained = sum(sizes.values())
    require(retained + additional <= limit, "combined accounted storage envelope exhausted")
    free = shutil.disk_usage(output).free
    require(free - additional >= 32 * 1024**3, "32 GiB free-space reserve would be exhausted")
    return {"combined_bytes": retained, "free_bytes": free, "accounted_roots": sizes}


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--seed-root", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path, help="fresh evidence directory")
    p.add_argument(
        "--account-root",
        action="append",
        type=Path,
        default=[],
        help="additional read-only legacy/artifact directory counted in the combined ceiling; repeat for every operator root outside repo/data (repo/data is counted automatically when present)",
    )
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--delta-markets", type=int, default=1000)
    p.add_argument("--delay-s", type=float, default=0.1)
    p.add_argument(
        "--executable",
        help="installed catalogue executable; default: this Python -m oddsfox_catalogue",
    )
    p.add_argument(
        "--max-retained-bytes",
        type=int,
        default=256 * 1024**3,
        help="combined seed/output/accounted roots envelope; cannot exceed 256 GiB",
    )
    return p


def run(args):
    seed = validate_root(args.seed_root)
    output = safe_path(args.output)
    require(not output.exists(), "output must be a fresh directory")
    require(
        seed not in output.parents and output not in seed.parents,
        "seed and output must be disjoint",
    )
    require(
        args.runs > 0 and args.delta_markets >= 12, "positive runs and >=12 delta markets required"
    )
    require(
        math.isfinite(args.delay_s) and args.delay_s >= 0, "delay must be finite and nonnegative"
    )
    require(0 < args.max_retained_bytes <= 256 * 1024**3, "combined ceiling must be <=256 GiB")
    account_roots = [safe_path(path) for path in args.account_root]
    require(
        all(path.is_dir() for path in account_roots),
        "every --account-root must be an existing directory",
    )
    if (REPO / "data").exists():
        account_roots.append(safe_path(REPO / "data"))
    output.mkdir(parents=True)
    report = {
        "kind": "synthetic-delta",
        "frozen_provider_policy": {
            "events": {"passes": [None]},
            "markets": {
                "default_omits_closed": True,
                "unknown_closed_in_false_pass": True,
                "passes": [False, True],
            },
        },
        "account_roots": [str(path) for path in accounting_roots(account_roots)],
        "passed": False,
        "runs": [],
        "limitations": "No historical network claim; six-hour/monthly reconciliation remains unmeasured.",
        "hardware": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
        },
        "cache_policy": "OS caches not flushed; equivalent serial copies, source frozen before timing",
        "settings": asdict(CurrentSettings(seed)),
        "lock_sha256": digest(REPO / "uv.lock"),
    }
    try:
        report["distribution"] = {
            "version": importlib.metadata.version("oddsfox-catalogue"),
            "location": str(importlib.metadata.distribution("oddsfox-catalogue").locate_file("")),
            "executable": args.executable or sys.executable,
        }
        distribution = importlib.metadata.distribution("oddsfox-catalogue")
        record = distribution.read_text("RECORD")
        report["distribution"]["record_sha256"] = (
            hashlib.sha256(record.encode()).hexdigest() if record else None
        )
        report["distribution"]["direct_url"] = distribution.read_text("direct_url.json")
        if args.executable:
            report["distribution"]["executable_sha256"] = digest(Path(args.executable).resolve())
        settings = CurrentSettings(seed)
        for name in ("catalogue.duckdb", "published", "config.toml"):
            path = seed / name
            if path.exists():
                safe_path(path)
                if path.is_dir():
                    for child in path.rglob("*"):
                        safe_path(child)
        require(not (seed / "catalogue.duckdb.wal").exists(), "seed must be checkpointed and idle")
        with duckdb.connect(str(settings.store_path), read_only=True) as conn:
            conn.execute("SET memory_limit='2GB'")
            conn.execute("SET threads=2")
            validate_store(conn)
            verified = verify_current(settings, connection=conn)
            discovery = get_state(conn, "discovery")
            require(
                discovery and discovery["pending"] is None and not get_state(conn, "fetch_pending"),
                "seed has pending work",
            )
            require(
                all(v["highwater"] is None for v in discovery["lanes"].values()),
                "seed has unfinished discovery",
            )
            require(
                verified["published_generation"] == verified["store_generation"],
                "seed publication is behind its store",
            )
            require(
                directory_bytes(seed) <= settings.limits.max_retained_bytes,
                "seed exceeds 64 GiB retained ceiling",
            )
            frontiers = {key: value["frontier"] for key, value in discovery["lanes"].items()}
            retained = retained_maxima(conn)
            started = time.monotonic()
            events, markets, controls, new_events, new_markets = build_source(
                conn, frontiers, args.delta_markets
            )
            delta = expected_delta(events, markets, new_events, new_markets)
            report["source_preparation_s"] = time.monotonic() - started
            report["seed_field_distributions"] = {
                field: conn.execute(
                    f"SELECT {field}, count(*) FROM markets GROUP BY {field} ORDER BY count(*) DESC"
                ).fetchall()
                for field in ("source_market_version", "active", "closed", "archived", "usable")
            }
            report["seed"] = {
                "release": verified,
                "frontiers": frontiers,
                "retained_maxima": retained,
                "retained_ahead_of_frontier": {
                    lane: max(0, retained[lane] - frontiers[lane]) for lane in frontiers
                },
                "manifest_sha256": digest(Path(verified["path"]) / "manifest.json"),
            }
            frozen_source = json.dumps([events, markets], sort_keys=True)
            (output / "source.json").write_text(frozen_source)
            report["source"] = {
                "payload_file": "source.json",
                "new_events": len(new_events),
                "new_markets": len(new_markets),
                "selected_existing_events": len(events) - len(new_events),
                "delay_s": args.delay_s,
                "sha256": hashlib.sha256(frozen_source.encode()).hexdigest(),
            }
        seed_size = sum(
            directory_bytes(seed / name) if (seed / name).is_dir() else (seed / name).stat().st_size
            for name in ("catalogue.json", "catalogue.duckdb", "published")
        )
        for index in range(args.runs):
            row = {"run": index + 1}
            report["runs"].append(row)
            row["copy_bytes"] = seed_size
            # Reserve the full canonical run ceiling, including publication/spill.
            row["capacity_before"] = capacity(
                seed, output, args.max_retained_bytes, 64 * 1024**3, account_roots=account_roots
            )
            root = output / f"run-{index + 1}"
            root.mkdir()
            started = time.monotonic()
            for name in ("catalogue.json", "catalogue.duckdb", "published", "config.toml"):
                source = seed / name
                if source.is_dir():
                    shutil.copytree(source, root / name, symlinks=False)
                elif source.exists():
                    shutil.copy2(source, root / name)
            row["copy_preparation_s"] = time.monotonic() - started
            with CurrentGammaServer(
                events, markets, delay_s=args.delay_s, market_default_open=True
            ) as source:
                options = [
                    part for key, value in controls.items() for part in (f"--{key}-id", value)
                ]
                row["probe"] = cli(root, source.url, args.executable, "probe", *options)
                certificate = successful(row["probe"])
                row["frozen_provider_policy"] = certificate["lanes"]
                before = source.stats["http_attempts"]
                row["refresh"] = cli(root, source.url, args.executable, "refresh")
                row["refresh_http"] = source.stats["responses"][before:]
                row["retained_gap_discovery"] = gap_discovery(
                    row["refresh_http"], frontiers, retained, row["refresh"]["elapsed_wall_s"]
                )
                result = successful(row["refresh"])
                require(
                    row["refresh"]["elapsed_wall_s"] < 300, "normal refresh exceeded 300 seconds"
                )
                require(
                    not result.get("pending") and result.get("backlog") == 0,
                    "delta was not fully applied",
                )
                require(
                    result["http_attempts"] == len(row["refresh_http"]),
                    "request accounting mismatch",
                )
                row["verify"] = cli(root, source.url, args.executable, "verify")
                successful(row["verify"])
                with connect_store(CurrentSettings(root)) as conn:
                    row["exact_counts"] = verify_expected(conn, Path(verified["path"]), delta)
                    row["sql_generation"] = get_state(conn, "generation")
                before = source.stats["http_attempts"]
                row["unchanged"] = cli(root, source.url, args.executable, "refresh")
                unchanged = successful(row["unchanged"])
                require(
                    row["unchanged"]["elapsed_wall_s"] < 300,
                    "unchanged refresh exceeded 300 seconds",
                )
                require(
                    unchanged["publication"]["noop"]
                    and unchanged["publication"]["release_id"]
                    == result["publication"]["release_id"],
                    "unchanged source changed publication",
                )
                row["unchanged_http"] = source.stats["responses"][before:]
                verify_highwater_only(row["unchanged_http"], certificate)
                row["final_verify"] = cli(root, source.url, args.executable, "verify")
                successful(row["final_verify"])
                row["http"] = source.stats
                row["retry_responses"] = sum(r["status"] >= 429 for r in source.stats["responses"])
            row["files"] = {
                str(p.relative_to(root)): p.stat().st_size for p in root.rglob("*") if p.is_file()
            }
            row["capacity_after"] = capacity(
                seed, output, args.max_retained_bytes, account_roots=account_roots
            )
            row["verified"] = True
            (output / f"run-{index + 1}.json").write_text(json.dumps(row, indent=2, default=str))
            shutil.rmtree(root)
        report["passed"] = True
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        (output / "report.json").write_text(json.dumps(report, indent=2, default=str))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(run(parser().parse_args()))
