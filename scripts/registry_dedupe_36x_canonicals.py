#!/usr/bin/env python3
"""
One-shot dedupe of detail_table_registry_v1 for Script 361 / 362 canonicals.

Background
----------
Surfaced during Script 363 handoff verification (see
``cursor_prompt_script_363_invasion_v2.md``). The Step 6 / Step 8 registry
sync helpers in Scripts 361 and 362 ran their INSERT step multiple times
without a DELETE-first idempotency clause that scoped to
``canonical_version``, leaving the registry with duplicate and partly stale
rows for every canonical built by those two scripts.

Pre-state (verified 2026-04-22):
    18 rows for canonical_version='v1_0_script361'  (should be 6)
     6 rows for canonical_version='v1_0_script362'  (should be 3)

Some duplicate rows additionally captured stale interim row counts (e.g.
``canonical_path_malignant_events_v1`` had a row at 11,106 / 8,422 patients
from an earlier 361 build alongside the current 6,689 / 4,137 row).

Behavior
--------
1. Connect to MotherDuck (RW token via ``motherduck_client.get_token``).
2. Snapshot the current ``v1_0_script361`` / ``v1_0_script362`` rows to
   ``archive_pub_v1_0.detail_table_registry_v1_pre363dedupe_<BUILD_TS>``
   (idempotent — skipped if a same-named snapshot already matches).
3. DELETE all registry rows with ``canonical_version IN
   ('v1_0_script361','v1_0_script362')``.
4. INSERT one authoritative row per canonical, using live row / patient
   counts read from the canonical table itself (no historical drift).

Usage::

    python scripts/registry_dedupe_36x_canonicals.py --dry-run
    python scripts/registry_dedupe_36x_canonicals.py --commit

PHI rule: research_id only. Never logs note text or clinical narrative.
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

BUILD_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Authoritative row per canonical. Live counts are read at runtime — these
# tuples define identity (name, schema, grain, domain, version) only.
AUTHORITATIVE_ROWS: list[dict[str, Any]] = [
    # Script 361 — operative pathology (6 canonicals)
    {"detail_table_name": "canonical_path_malignant_events_v1",
     "schema_name": "main", "grain": "per_tumor_per_surgery",
     "domain": "operative_pathology", "canonical_version": "v1_0_script361"},
    {"detail_table_name": "canonical_path_benign_events_v1",
     "schema_name": "main", "grain": "per_synoptic_report",
     "domain": "operative_pathology", "canonical_version": "v1_0_script361"},
    {"detail_table_name": "canonical_path_gland_events_v1",
     "schema_name": "main", "grain": "per_gland_per_surgery",
     "domain": "operative_pathology", "canonical_version": "v1_0_script361"},
    {"detail_table_name": "canonical_path_malignant_patient_rollup_v1",
     "schema_name": "main", "grain": "per_patient",
     "domain": "operative_pathology", "canonical_version": "v1_0_script361"},
    {"detail_table_name": "canonical_path_benign_patient_rollup_v1",
     "schema_name": "main", "grain": "per_patient",
     "domain": "operative_pathology", "canonical_version": "v1_0_script361"},
    {"detail_table_name": "canonical_path_gland_patient_rollup_v1",
     "schema_name": "main", "grain": "per_patient",
     "domain": "operative_pathology", "canonical_version": "v1_0_script361"},
    # Script 362 — operative procedure (3 canonicals)
    {"detail_table_name": "canonical_operative_events_v1",
     "schema_name": "main", "grain": "per_surgery_episode",
     "domain": "operative_procedure", "canonical_version": "v1_0_script362"},
    {"detail_table_name": "canonical_operative_patient_rollup_v1",
     "schema_name": "main", "grain": "per_patient",
     "domain": "operative_procedure", "canonical_version": "v1_0_script362"},
    {"detail_table_name": "canonical_operative_procedure_codes_v1",
     "schema_name": "main", "grain": "per_procedure_mention",
     "domain": "operative_procedure", "canonical_version": "v1_0_script362"},
]

TARGET_VERSIONS = ("v1_0_script361", "v1_0_script362")
EXPECTED_PRE_COUNT = 24  # 18 + 6 — guard against unexpected schema state.
EXPECTED_POST_COUNT = len(AUTHORITATIVE_ROWS)  # 9


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


def get_registry_columns(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [CANONICAL_DB, WS_SCHEMA, REGISTRY_TABLE],
    ).fetchall()
    return [r[0] for r in rows]


def snapshot_pre_state(
    con: duckdb.DuckDBPyConnection, do_writes: bool
) -> str | None:
    snapshot_name = f"{REGISTRY_TABLE}_pre363dedupe_{BUILD_TS}"
    src = fq_canonical(WS_SCHEMA, REGISTRY_TABLE)
    dst = fq_archive(snapshot_name)

    n_full = int(con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0])

    placeholders = ", ".join(["?"] * len(TARGET_VERSIONS))
    n_target = int(con.execute(
        f"SELECT COUNT(*) FROM {src} "
        f"WHERE canonical_version IN ({placeholders})",
        list(TARGET_VERSIONS),
    ).fetchone()[0])

    log(f"  registry currently has {n_full} total rows; "
        f"{n_target} target (361+362) rows")
    if n_target != EXPECTED_PRE_COUNT:
        log(f"  WARN: expected {EXPECTED_PRE_COUNT} target rows, found "
            f"{n_target}. Proceeding anyway — script idempotently rebuilds "
            f"to {EXPECTED_POST_COUNT} authoritative rows.")

    log(f"  snapshot plan: {dst}")
    if not do_writes:
        return None

    existing = con.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
        """,
        [ARCHIVE_DB, ARCHIVE_SCHEMA, snapshot_name],
    ).fetchone()
    if existing:
        log("  snapshot already exists — skipping create")
        return snapshot_name

    con.execute(f"CREATE TABLE {dst} AS SELECT * FROM {src}")
    n_dst = int(con.execute(f"SELECT COUNT(*) FROM {dst}").fetchone()[0])
    if n_dst != n_full:
        raise RuntimeError(
            f"Snapshot mismatch: src={n_full} dst={n_dst}"
        )
    try:
        con.execute(
            f"COMMENT ON TABLE {dst} IS "
            f"'Pre-Script-363 dedupe snapshot of {WS_SCHEMA}.{REGISTRY_TABLE} "
            f"({RUN_DATE}). Captures the pre-state before deduping rows for "
            f"v1_0_script361 / v1_0_script362.'"
        )
    except duckdb.Error as exc:
        log(f"  COMMENT ON snapshot failed (non-fatal): {exc}")
    log(f"  snapshot created -> {snapshot_name} ({n_dst} rows)")
    return snapshot_name


def fetch_live_counts(
    con: duckdb.DuckDBPyConnection,
) -> dict[str, tuple[int, int]]:
    out: dict[str, tuple[int, int]] = {}
    for rec in AUTHORITATIVE_ROWS:
        sch, tbl = rec["schema_name"], rec["detail_table_name"]
        n = int(con.execute(
            f"SELECT COUNT(*) FROM {fq_canonical(sch, tbl)}"
        ).fetchone()[0])
        p = int(con.execute(
            f"SELECT COUNT(DISTINCT research_id) FROM {fq_canonical(sch, tbl)}"
        ).fetchone()[0])
        out[tbl] = (n, p)
    return out


def build_insert_record(
    rec: dict[str, Any], live: tuple[int, int],
) -> dict[str, Any]:
    n, p = live
    grain = rec["grain"]
    ver = rec["canonical_version"]
    script_id = ver.replace("v1_0_script", "")
    return {
        "detail_table_name":              rec["detail_table_name"],
        "schema_name":                    rec["schema_name"],
        "join_key":                       "research_id",
        "grain":                          grain,
        "total_rows":                     n,
        "total_patients":                 p,
        "domain":                         rec["domain"],
        "feeds_master_columns":           None,
        "description": (
            f"[domain={rec['domain']}; grain={grain}] — source: "
            f"Script {script_id} ({RUN_DATE}). Rows={n}, patients={p}. "
            f"Registry deduped {RUN_DATE} via "
            f"scripts/registry_dedupe_36x_canonicals.py."
        ),
        "canonical_version":              ver,
        "feeds_master_columns_secondary": None,
        "feeds_master_columns_array":     None,
        "needs_manual_review":            False,
    }


def apply_dedupe(
    con: duckdb.DuckDBPyConnection,
    do_writes: bool,
) -> dict[str, Any]:
    reg_cols = get_registry_columns(con)
    log(f"  registry has columns: {reg_cols}")

    live_counts = fetch_live_counts(con)
    log(f"  fetched live counts for {len(live_counts)} canonicals")
    for tbl, (n, p) in live_counts.items():
        log(f"    {tbl}: {n:,} rows / {p:,} patients (live)")

    placeholders = ", ".join(["?"] * len(TARGET_VERSIONS))
    n_pre = int(con.execute(
        f"SELECT COUNT(*) FROM {fq_canonical(WS_SCHEMA, REGISTRY_TABLE)} "
        f"WHERE canonical_version IN ({placeholders})",
        list(TARGET_VERSIONS),
    ).fetchone()[0])
    log(f"  DELETE plan: {n_pre} rows where canonical_version IN "
        f"{TARGET_VERSIONS}")

    inserts: list[tuple[str, list[Any]]] = []
    for rec in AUTHORITATIVE_ROWS:
        live = live_counts[rec["detail_table_name"]]
        insert_rec = build_insert_record(rec, live)
        ordered = [(c, insert_rec[c]) for c in reg_cols if c in insert_rec]
        col_csv = ", ".join(c for c, _ in ordered)
        ph_csv = ", ".join("?" for _ in ordered)
        sql = (f"INSERT INTO {fq_canonical(WS_SCHEMA, REGISTRY_TABLE)} "
               f"({col_csv}) VALUES ({ph_csv})")
        inserts.append((sql, [v for _, v in ordered]))
        log(f"    INSERT {rec['detail_table_name']} "
            f"({rec['canonical_version']}) "
            f"rows={insert_rec['total_rows']:,} "
            f"patients={insert_rec['total_patients']:,}")

    if not do_writes:
        log("  [dry-run] no DELETE / INSERT executed")
        return {"deleted": n_pre, "inserted": len(inserts), "applied": False}

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            f"DELETE FROM {fq_canonical(WS_SCHEMA, REGISTRY_TABLE)} "
            f"WHERE canonical_version IN ({placeholders})",
            list(TARGET_VERSIONS),
        )
        for sql, params in inserts:
            con.execute(sql, params)
        con.execute("COMMIT")
        log(f"  COMMITTED: deleted {n_pre} rows, inserted {len(inserts)} rows")
    except Exception:
        con.execute("ROLLBACK")
        raise

    n_post = int(con.execute(
        f"SELECT COUNT(*) FROM {fq_canonical(WS_SCHEMA, REGISTRY_TABLE)} "
        f"WHERE canonical_version IN ({placeholders})",
        list(TARGET_VERSIONS),
    ).fetchone()[0])
    log(f"  post-state: {n_post} rows for {TARGET_VERSIONS}")
    if n_post != EXPECTED_POST_COUNT:
        raise RuntimeError(
            f"Post-state row count {n_post} != expected {EXPECTED_POST_COUNT}"
        )
    return {"deleted": n_pre, "inserted": len(inserts), "applied": True,
            "post_count": n_post}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Dedupe detail_table_registry_v1 rows for "
                    "Script 361 / 362 canonicals."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--commit", action="store_true",
                      help="Apply changes (snapshot + DELETE + INSERT).")
    mode.add_argument("--dry-run", action="store_true",
                      help="Plan only — print intended SQL, no writes.")
    args = parser.parse_args()

    do_writes = bool(args.commit)
    log(f"Run config: do_writes={do_writes} BUILD_TS={BUILD_TS}")

    con = connect()
    log("STEP 1 — Snapshot pre-state to archive_pub_v1_0")
    snapshot = snapshot_pre_state(con, do_writes)
    log(f"  snapshot result: {snapshot}")

    log("STEP 2 — Apply dedupe (DELETE + INSERT in one transaction)")
    result = apply_dedupe(con, do_writes)
    log(f"  result: {result}")

    log("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
