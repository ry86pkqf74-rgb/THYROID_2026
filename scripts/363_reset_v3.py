#!/usr/bin/env python3
"""
Script 363 — RESET for v3 rebuild.

One-shot teardown of the v2 build (commit 0fd2411) so v3 can rebuild
from a clean slate per Logan's rejection findings:
  1. Cross-DB violation — v2 sourced from archive_pub_v1_0.*; rejected.
  2. Classification bug — invasion_type='local' bundles V/L/capsular/
     perineural/soft_tissue + dumps mass-effect entities (tracheal
     deviation, substernal extension, esophageal compression, etc.).
  3. V/L aggregation — vascular_microscopic must be V-only, with a new
     lymphatic_microscopic type for L.
  4. Pattern 9 violation — build_ts is TIMESTAMP WITH TIME ZONE; must
     be CAST(CURRENT_TIMESTAMP AS TIMESTAMP).

Tear-down order (idempotent):
  1. Snapshot main.canonical_invasion_events_v1 (82,398 rows) →
     archive_pub_v1_0.canonical_invasion_events_v1_pre363v3_<BUILD_TS>
  2. Snapshot main.canonical_invasion_patient_rollup_v1 (10,871 rows) →
     archive_pub_v1_0.canonical_invasion_patient_rollup_v1_pre363v3_<BUILD_TS>
  3. DROP VIEW views_readable.invasion_events_VIEW_v1
  4. DROP VIEW views_readable.invasion_patient_rollup_VIEW_v1
  5. DROP TABLE main.canonical_invasion_events_v1
  6. DROP TABLE main.canonical_invasion_patient_rollup_v1
  7. DELETE FROM manuscript_workspace.detail_table_registry_v1
       WHERE canonical_version = 'v1_0_script363'

Usage::

    python scripts/363_reset_v3.py --dry-run
    python scripts/363_reset_v3.py --commit

PHI rule: research_id only.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from motherduck_client import get_token, token_mode  # noqa: E402

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
WS_SCHEMA = "manuscript_workspace"
REGISTRY_TABLE = "detail_table_registry_v1"
VIEW_SCHEMA = "views_readable"
TARGET_VERSION = "v1_0_script363"

BUILD_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Two new canonicals + two views to drop.
CANONICALS_TO_DROP: list[tuple[str, str]] = [
    ("main", "canonical_invasion_events_v1"),
    ("main", "canonical_invasion_patient_rollup_v1"),
]
VIEWS_TO_DROP: list[tuple[str, str]] = [
    (VIEW_SCHEMA, "invasion_events_VIEW_v1"),
    (VIEW_SCHEMA, "invasion_patient_rollup_VIEW_v1"),
]


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}Z] {msg}", flush=True)


def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token (token_mode={token_mode()}). "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    log(f"Connecting md:{CANONICAL_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")
    con.execute(f'USE "{CANONICAL_DB}"')
    return con


def fq_canonical(schema: str, name: str) -> str:
    return f'"{CANONICAL_DB}"."{schema}"."{name}"'


def fq_archive(name: str) -> str:
    return f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"."{name}"'


def table_exists(con: duckdb.DuckDBPyConnection, schema: str,
                 name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name=?",
        [CANONICAL_DB, schema, name],
    ).fetchone()
    return row is not None


def view_exists(con: duckdb.DuckDBPyConnection, schema: str,
                name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema=? AND table_name=? "
        "AND table_type='VIEW'",
        [CANONICAL_DB, schema, name],
    ).fetchone()
    return row is not None


def archive_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    con.execute(f'USE "{ARCHIVE_DB}"')
    row = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema=? AND table_name=?",
        [ARCHIVE_SCHEMA, name],
    ).fetchone()
    con.execute(f'USE "{CANONICAL_DB}"')
    return row is not None


def step_1_snapshot(con: duckdb.DuckDBPyConnection,
                    do_writes: bool) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 1 — Snapshot v2 canonicals to archive_pub_v1_0")
    log("=" * 78)
    snapshots: list[dict[str, Any]] = []
    for schema, name in CANONICALS_TO_DROP:
        if not table_exists(con, schema, name):
            log(f"  {schema}.{name} does not exist — nothing to snapshot")
            continue
        n = int(con.execute(
            f"SELECT COUNT(*) FROM {fq_canonical(schema, name)}"
        ).fetchone()[0])
        snapshot_name = f"{name}_pre363v3_{BUILD_TS}"
        log(f"  snapshot plan: {schema}.{name} ({n:,} rows) -> "
            f"{snapshot_name}")
        if not do_writes:
            snapshots.append({"src": f"{schema}.{name}",
                              "dst": snapshot_name, "rows": n,
                              "status": "DRY_RUN"})
            continue
        if archive_exists(con, snapshot_name):
            n_dst = int(con.execute(
                f"SELECT COUNT(*) FROM {fq_archive(snapshot_name)}"
            ).fetchone()[0])
            if n_dst != n:
                raise RuntimeError(
                    f"Snapshot {snapshot_name} exists with {n_dst:,} rows "
                    f"but live has {n:,}. Refusing to overwrite."
                )
            log(f"  snapshot already exists ({n_dst:,} rows) — skipping")
            snapshots.append({"src": f"{schema}.{name}",
                              "dst": snapshot_name, "rows": n_dst,
                              "status": "EXISTS"})
            continue
        con.execute(
            f"CREATE TABLE {fq_archive(snapshot_name)} AS "
            f"SELECT * FROM {fq_canonical(schema, name)}"
        )
        n_dst = int(con.execute(
            f"SELECT COUNT(*) FROM {fq_archive(snapshot_name)}"
        ).fetchone()[0])
        if n_dst != n:
            raise RuntimeError(
                f"Snapshot mismatch for {schema}.{name}: "
                f"src={n:,} dst={n_dst:,}"
            )
        try:
            con.execute(
                f"COMMENT ON TABLE {fq_archive(snapshot_name)} IS "
                f"'Pre-v3 snapshot of {schema}.{name} taken {RUN_DATE} "
                f"by scripts/363_reset_v3.py before v3 rebuild. "
                f"Tear-down rationale: cross-DB violation, classification "
                f"bug (vascular vs lymphatic conflated; mass-effect "
                f"entities dumped into local), Pattern 9 TIMESTAMPTZ "
                f"violation. See cursor_prompt_script_363_invasion_v3.md.'"
            )
        except duckdb.Error as exc:
            log(f"  COMMENT ON {snapshot_name} failed (non-fatal): {exc}")
        log(f"  snapshot created -> {snapshot_name} ({n_dst:,} rows)")
        snapshots.append({"src": f"{schema}.{name}",
                          "dst": snapshot_name, "rows": n_dst,
                          "status": "ARCHIVED"})
    return {"snapshots": snapshots}


def step_2_drop_views(con: duckdb.DuckDBPyConnection,
                      do_writes: bool) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 2 — Drop views")
    log("=" * 78)
    dropped: list[str] = []
    for schema, name in VIEWS_TO_DROP:
        if not view_exists(con, schema, name):
            log(f"  {schema}.{name} does not exist — skip")
            continue
        log(f"  DROP VIEW {schema}.{name}")
        if do_writes:
            con.execute(f"DROP VIEW {fq_canonical(schema, name)}")
        dropped.append(f"{schema}.{name}")
    return {"dropped_views": dropped}


def step_3_drop_tables(con: duckdb.DuckDBPyConnection,
                       do_writes: bool,
                       snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 3 — Drop canonicals")
    log("=" * 78)
    snapshot_lookup = {s["src"]: s for s in snapshots}
    dropped: list[str] = []
    for schema, name in CANONICALS_TO_DROP:
        if not table_exists(con, schema, name):
            log(f"  {schema}.{name} does not exist — skip")
            continue
        # Parity safety check: the snapshot must exist with matching
        # rows before we drop the live table.
        snap = snapshot_lookup.get(f"{schema}.{name}")
        if not snap or snap["status"] not in ("ARCHIVED", "EXISTS"):
            if do_writes:
                raise RuntimeError(
                    f"Refusing to drop {schema}.{name}: no parity-verified "
                    f"snapshot found. Snapshot status: {snap}"
                )
        log(f"  DROP TABLE {schema}.{name}")
        if do_writes:
            con.execute(f"DROP TABLE {fq_canonical(schema, name)}")
        dropped.append(f"{schema}.{name}")
    return {"dropped_tables": dropped}


def step_4_clear_registry(con: duckdb.DuckDBPyConnection,
                          do_writes: bool) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 4 — DELETE registry rows for v1_0_script363")
    log("=" * 78)
    n_pre = int(con.execute(
        f"SELECT COUNT(*) FROM {fq_canonical(WS_SCHEMA, REGISTRY_TABLE)} "
        f"WHERE canonical_version = ?", [TARGET_VERSION]
    ).fetchone()[0])
    log(f"  {n_pre} registry rows currently match canonical_version="
        f"'{TARGET_VERSION}'")
    if do_writes:
        con.execute(
            f"DELETE FROM {fq_canonical(WS_SCHEMA, REGISTRY_TABLE)} "
            f"WHERE canonical_version = ?", [TARGET_VERSION],
        )
        n_post = int(con.execute(
            f"SELECT COUNT(*) FROM {fq_canonical(WS_SCHEMA, REGISTRY_TABLE)} "
            f"WHERE canonical_version = ?", [TARGET_VERSION]
        ).fetchone()[0])
        if n_post != 0:
            raise RuntimeError(
                f"Post-DELETE: {n_post} rows still match "
                f"canonical_version='{TARGET_VERSION}'"
            )
        log(f"  DELETE OK ({n_pre} rows removed)")
    return {"deleted_registry_rows": n_pre}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reset v2 invasion canonicals for v3 rebuild."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--commit", action="store_true",
                      help="Apply snapshot + drops + registry delete.")
    mode.add_argument("--dry-run", action="store_true",
                      help="Plan only — print intended SQL, no writes.")
    args = parser.parse_args()

    do_writes = bool(args.commit)
    log(f"Run config: do_writes={do_writes} BUILD_TS={BUILD_TS}")

    con = connect()
    s1 = step_1_snapshot(con, do_writes)
    s2 = step_2_drop_views(con, do_writes)
    s3 = step_3_drop_tables(con, do_writes, s1["snapshots"])
    s4 = step_4_clear_registry(con, do_writes)

    log("=" * 78)
    log("DONE — reset complete (dry-run)" if not do_writes
        else "DONE — reset complete and committed")
    log(f"  snapshots: {[s['dst'] for s in s1['snapshots']]}")
    log(f"  views dropped: {s2['dropped_views']}")
    log(f"  tables dropped: {s3['dropped_tables']}")
    log(f"  registry rows deleted: {s4['deleted_registry_rows']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
