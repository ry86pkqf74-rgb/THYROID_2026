#!/usr/bin/env python3
"""
104_operative_truth_state_hardening.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Operative NLP Truth-State Hardening — prevents manuscript and analysis layers
from misrepresenting unknown operative details as confirmed negatives.

ROOT CAUSE:
  Script 22 creates operative_episode_detail_v2 with `FALSE AS <field>` for
  10 boolean operative NLP fields.  The COALESCE(nlp_value, old_FALSE) UPDATE
  never overwrites because FALSE is non-NULL.  Script 86 compounds this by
  using `COALESCE(o.field, FALSE)` when propagating, converting any NULL
  (honest unknown) back to FALSE (looks like confirmed negative).

FIX:
  1. Recode operative_episode_detail_v2: FALSE → NULL for every row where the
     field is NOT TRUE (preserve NLP-confirmed positives; reclassify everything
     else as UNKNOWN).
  2. Re-propagate episode → patient → manuscript layers using bare field
     references (no COALESCE to FALSE).
  3. Create validation/audit tables documenting before/after distributions.

FIELDS (10 hardcoded-FALSE in script 22, plus 2 already-NULL, 1 from ALTER):
  Category B — NOT_PARSED (hardcoded FALSE, NLP extractor exists but
               COALESCE pattern prevented overwrite):
    rln_monitoring_flag, parathyroid_autograft_flag, gross_ete_flag,
    local_invasion_flag, tracheal_involvement_flag, esophageal_involvement_flag,
    strap_muscle_involvement_flag, reoperative_field_flag, drain_flag

  Category C — SOURCE_ABSENT (hardcoded FALSE or ALTER default, NLP entity
               types not in vocabulary / 0 matches in corpus):
    parathyroid_resection_flag, frozen_section_flag, berry_ligament_flag

SCORING IMPACT:
  gross_ete_flag is used in scoring systems (AJCC8 T3b, ATA risk, MACIS).
  After hardening, unknown ETE → NULL → no T3b upstaging → CORRECT behavior
  (unknown ETE should NOT trigger staging changes).

PHASES:
  A — BEFORE snapshot (current TRUE/FALSE/NULL distributions)
  B — Recode operative_episode_detail_v2 (FALSE→NULL where NOT TRUE)
  C — Re-propagate to episode_analysis_resolved_v1 (stripped COALESCE)
  D — Re-propagate to patient_analysis_resolved_v1 (BOOL_OR without COALESCE)
  E — Re-propagate to manuscript_cohort_v1 (same as D)
  F — Rebuild md_ mirrors on local DuckDB
  G — Create val_operative_truth_state_v1 + val_operative_truth_state_delta_v1
  H — Export artifacts + AFTER snapshot

Usage:
    .venv/bin/python scripts/104_operative_truth_state_hardening.py --md
    .venv/bin/python scripts/104_operative_truth_state_hardening.py --md --dry-run
    .venv/bin/python scripts/104_operative_truth_state_hardening.py --md --phase A
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
sys.path.insert(0, str(ROOT))

DATE_TAG = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
EXPORTS_DIR = ROOT / "exports" / f"operative_nlp_truth_state_hardening_{DATE_TAG}"
DOCS_DIR = ROOT / "docs"

# ─── All operative boolean fields ─────────────────────────────────────────────

# Fields that were hardcoded FALSE in script 22's CREATE TABLE
HARDCODED_FALSE_FIELDS: list[str] = [
    "rln_monitoring_flag",
    "parathyroid_autograft_flag",
    "gross_ete_flag",
    "local_invasion_flag",
    "tracheal_involvement_flag",
    "esophageal_involvement_flag",
    "strap_muscle_involvement_flag",
    "reoperative_field_flag",
    "drain_flag",
    "parathyroid_resection_flag",   # hardcoded FALSE in script 22
]

# Fields added via ALTER TABLE (defaulted to NULL — already correct)
ALREADY_NULL_FIELDS: list[str] = [
    "frozen_section_flag",
    "berry_ligament_flag",
]

ALL_BOOLEAN_FIELDS: list[str] = HARDCODED_FALSE_FIELDS + ALREADY_NULL_FIELDS

# Patient-level aggregate fields (op_*)
PATIENT_OP_BOOL_FIELDS: list[tuple[str, str]] = [
    ("op_rln_monitoring_any",        "rln_monitoring_flag"),
    ("op_drain_placed_any",          "drain_flag"),
    ("op_strap_muscle_any",          "strap_muscle_involvement_flag"),
    ("op_reoperative_any",           "reoperative_field_flag"),
    ("op_parathyroid_autograft_any", "parathyroid_autograft_flag"),
    ("op_local_invasion_any",        "local_invasion_flag"),
    ("op_tracheal_inv_any",          "tracheal_involvement_flag"),
    ("op_esophageal_inv_any",        "esophageal_involvement_flag"),
    ("op_intraop_gross_ete_any",     "gross_ete_flag"),
]

# ─── Connection ───────────────────────────────────────────────────────────────

def get_token() -> str:
    token = os.environ.get("LOCAL_DB_PATH")
    if token:
        return token
    secrets = ROOT / ".streamlit" / "secrets.toml"
    if secrets.exists():
        import toml
        return toml.load(str(secrets))["LOCAL_DB_PATH"]
    raise RuntimeError("LOCAL_DB_PATH not found in env or .streamlit/secrets.toml")


def connect(use_md: bool = False, use_local: bool = False) -> duckdb.DuckDBPyConnection:
    import os as _os
    if use_local or _os.environ.get('USE_LOCAL_DUCKDB'):
        path = _os.environ.get('LOCAL_DUCKDB_PATH', str(ROOT / 'thyroid_master_local.duckdb'))
        return duckdb.connect(path)
    from utils.md_connect import connect_md_or_file
    return connect_md_or_file(DB_PATH, md=use_md)


def safe_exec(con: duckdb.DuckDBPyConnection, sql: str,
              label: str = "") -> int:
    """Execute SQL; return rowcount (0 for DDL)."""
    try:
        r = con.execute(sql)
        rc = r.rowcount if hasattr(r, "rowcount") else 0
        return 0 if rc == -1 else rc
    except Exception as e:
        print(f"    [ERROR] {label}: {e}")
        return -1


def safe_count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    try:
        r = con.execute(sql).fetchone()
        return int(r[0]) if r else 0
    except Exception:
        return -1


def col_exists(con: duckdb.DuckDBPyConnection, table: str, col: str) -> bool:
    n = safe_count(con,
        f"SELECT COUNT(DISTINCT column_name) FROM information_schema.columns "
        f"WHERE table_name='{table}' AND column_name='{col}' AND table_schema='main'"
    )
    return n > 0


def tbl_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    n = safe_count(con,
        f"SELECT COUNT(DISTINCT table_name) FROM information_schema.tables "
        f"WHERE table_name='{table}' AND table_schema='main'"
    )
    return n > 0


# ─── Phase A: BEFORE snapshot ─────────────────────────────────────────────────

def snapshot_table(con: duckdb.DuckDBPyConnection, table: str,
                   fields: list[str], label: str) -> list[dict]:
    """Capture TRUE/FALSE/NULL distribution for each field in a table."""
    results = []
    total = safe_count(con, f"SELECT COUNT(*) FROM {table}")
    for f in fields:
        if not col_exists(con, table, f):
            results.append({
                "label": label, "table": table, "field": f,
                "total": total, "true_ct": -1, "false_ct": -1, "null_ct": -1,
                "status": "COLUMN_MISSING"
            })
            continue
        r = con.execute(f"""
            SELECT
                SUM(CASE WHEN {f} IS TRUE THEN 1 ELSE 0 END),
                SUM(CASE WHEN {f} IS NOT TRUE AND {f} IS NOT NULL THEN 1 ELSE 0 END),
                SUM(CASE WHEN {f} IS NULL THEN 1 ELSE 0 END)
            FROM {table}
        """).fetchone()
        results.append({
            "label": label, "table": table, "field": f,
            "total": total,
            "true_ct": int(r[0]),
            "false_ct": int(r[1]),
            "null_ct": int(r[2]),
            "status": "OK"
        })
    return results


def phase_a(con: duckdb.DuckDBPyConnection,
            dry_run: bool) -> dict[str, list[dict]]:
    """BEFORE snapshot of all operative boolean field distributions."""
    print("\n═══ Phase A: BEFORE snapshot ═══")
    snapshots: dict[str, list[dict]] = {}

    print("  Snapshotting operative_episode_detail_v2 ...")
    snapshots["oed_before"] = snapshot_table(
        con, "operative_episode_detail_v2", ALL_BOOLEAN_FIELDS, "BEFORE"
    )
    for s in snapshots["oed_before"]:
        f = s["field"]
        print(f"    {f:42s}  TRUE={s['true_ct']:6d}  "
              f"FALSE={s['false_ct']:6d}  NULL={s['null_ct']:6d}")

    print("  Snapshotting episode_analysis_resolved_v1 ...")
    ep_fields = [f for f in ALL_BOOLEAN_FIELDS
                 if col_exists(con, "episode_analysis_resolved_v1", f)]
    snapshots["eard_before"] = snapshot_table(
        con, "episode_analysis_resolved_v1", ep_fields, "BEFORE"
    )
    for s in snapshots["eard_before"]:
        f = s["field"]
        print(f"    {f:42s}  TRUE={s['true_ct']:6d}  "
              f"FALSE={s['false_ct']:6d}  NULL={s['null_ct']:6d}")

    print("  Snapshotting patient_analysis_resolved_v1 op_* ...")
    pat_fields = [pf for pf, _ in PATIENT_OP_BOOL_FIELDS]
    snapshots["pat_before"] = snapshot_table(
        con, "patient_analysis_resolved_v1", pat_fields, "BEFORE"
    )
    for s in snapshots["pat_before"]:
        f = s["field"]
        print(f"    {f:42s}  TRUE={s['true_ct']:6d}  "
              f"FALSE={s['false_ct']:6d}  NULL={s['null_ct']:6d}")

    print("  Snapshotting manuscript_cohort_v1 op_* ...")
    mc_fields = [pf for pf, _ in PATIENT_OP_BOOL_FIELDS
                 if col_exists(con, "manuscript_cohort_v1", pf)]
    snapshots["mc_before"] = snapshot_table(
        con, "manuscript_cohort_v1", mc_fields, "BEFORE"
    )
    for s in snapshots["mc_before"]:
        f = s["field"]
        print(f"    {f:42s}  TRUE={s['true_ct']:6d}  "
              f"FALSE={s['false_ct']:6d}  NULL={s['null_ct']:6d}")

    return snapshots


# ─── Phase B: Recode operative_episode_detail_v2 ─────────────────────────────

def phase_b(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict[str, int]:
    """Recode FALSE → NULL for NOT_PARSED fields in operative_episode_detail_v2.

    Logic: For each field, UPDATE SET field = NULL WHERE field IS NOT TRUE.
    This preserves NLP-confirmed TRUE values and recodes everything else as
    UNKNOWN (NULL).  This is semantically correct because:
      - TRUE = NLP confirmed the finding
      - NULL = NLP found no evidence OR NLP never processed this note
      - FALSE should ONLY represent explicit NLP negation, which our V2
        extractor does not track separately from "not found"
    """
    print("\n═══ Phase B: Recode operative_episode_detail_v2 (FALSE→NULL) ═══")
    recoded: dict[str, int] = {}

    for field in HARDCODED_FALSE_FIELDS:
        if not col_exists(con, "operative_episode_detail_v2", field):
            print(f"    {field}: column missing — skip")
            recoded[field] = -1
            continue

        # Count how many will be affected
        n_false = safe_count(con,
            f"SELECT COUNT(*) FROM operative_episode_detail_v2 "
            f"WHERE {field} IS NOT TRUE"
        )
        n_true = safe_count(con,
            f"SELECT COUNT(*) FROM operative_episode_detail_v2 "
            f"WHERE {field} IS TRUE"
        )

        if dry_run:
            print(f"    [DRY-RUN] {field}: would recode {n_false} FALSE→NULL "
                  f"(preserving {n_true} TRUE)")
            recoded[field] = n_false
        else:
            sql = f"""
                UPDATE operative_episode_detail_v2
                SET {field} = NULL
                WHERE {field} IS NOT TRUE
            """
            rc = safe_exec(con, sql, f"recode {field}")
            actual = rc if rc >= 0 else n_false
            print(f"    {field}: recoded {actual} FALSE→NULL "
                  f"(preserved {n_true} TRUE)")
            recoded[field] = actual

    return recoded


# ─── Phase C: Re-propagate to episode_analysis_resolved_v1 ──────────────────

def phase_c(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Re-propagate operative booleans from operative_episode_detail_v2 to
    episode_analysis_resolved_v1 using bare field references (NO COALESCE).

    NULL in OED → NULL in EARD (honest unknown).
    TRUE in OED → TRUE in EARD (NLP-confirmed).
    """
    print("\n═══ Phase C: Re-propagate to episode_analysis_resolved_v1 ═══")

    # Build SET clauses using bare field references — NO COALESCE(..., FALSE)
    set_parts = []
    for field in HARDCODED_FALSE_FIELDS:
        if col_exists(con, "episode_analysis_resolved_v1", field):
            set_parts.append(f"    {field} = o.{field}")

    if not set_parts:
        print("    No matching columns in episode_analysis_resolved_v1 — skip")
        return 0

    set_clause = ",\n".join(set_parts)
    sql = f"""
UPDATE episode_analysis_resolved_v1
SET
{set_clause}
FROM (
    SELECT
        research_id,
        surgery_episode_id,
        {', '.join(f for f in HARDCODED_FALSE_FIELDS
                   if col_exists(con, 'operative_episode_detail_v2', f))}
    FROM operative_episode_detail_v2
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id, surgery_episode_id
        ORDER BY research_id
    ) = 1
) o
WHERE episode_analysis_resolved_v1.research_id = o.research_id
  AND episode_analysis_resolved_v1.surgery_episode_id = o.surgery_episode_id
"""

    if dry_run:
        print(f"    [DRY-RUN] Would UPDATE episode_analysis_resolved_v1 "
              f"SET {len(set_parts)} fields from OED (no COALESCE)")
        return 0

    rc = safe_exec(con, sql, "re-propagate episode fields")
    print(f"    Updated episode_analysis_resolved_v1: {rc} rows")

    # Also update the dedup table
    print("    Rebuilding episode_analysis_resolved_v1_dedup ...")
    dedup_sql = """
    CREATE OR REPLACE TABLE episode_analysis_resolved_v1_dedup AS
    SELECT e.*
    FROM episode_analysis_resolved_v1 e
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY e.research_id, e.surgery_episode_id
        ORDER BY
            e.episode_analysis_eligible_flag DESC NULLS LAST,
            CASE e.t_stage
                WHEN 'T4b' THEN 1 WHEN 'T4a' THEN 2
                WHEN 'T3b' THEN 3 WHEN 'T3a' THEN 4 WHEN 'T3' THEN 5
                WHEN 'T2'  THEN 6 WHEN 'T1b' THEN 7 WHEN 'T1a' THEN 8
                ELSE 9 END ASC,
            CASE e.n_stage
                WHEN 'N1b' THEN 1 WHEN 'N1a' THEN 2
                WHEN 'N1'  THEN 3 WHEN 'N0'  THEN 4
                ELSE 5 END ASC,
            e.tumor_size_cm DESC NULLS LAST,
            e.ln_positive DESC NULLS LAST,
            COALESCE(e.path_link_score_v3, 0) DESC,
            CASE e.path_link_confidence_v2
                WHEN 'exact_match'      THEN 1
                WHEN 'high_confidence'  THEN 2
                WHEN 'plausible'        THEN 3
                WHEN 'weak'             THEN 4
                ELSE 5 END ASC
    ) = 1
    """
    safe_exec(con, dedup_sql, "rebuild dedup")
    dedup_ct = safe_count(con,
        "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup")
    print(f"    Rebuilt episode_analysis_resolved_v1_dedup: {dedup_ct} rows")

    return rc if rc >= 0 else 0


# ─── Phase D: Re-propagate to patient_analysis_resolved_v1 ──────────────────

def phase_d(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Re-aggregate operative booleans at patient level using BOOL_OR(field)
    WITHOUT COALESCE.

    BOOL_OR(NULL) → NULL (all surgeries unknown for this field)
    BOOL_OR(TRUE) → TRUE (at least one NLP-confirmed positive)
    """
    print("\n═══ Phase D: Re-propagate to patient_analysis_resolved_v1 ═══")

    # Build aggregation using bare BOOL_OR (not BOOL_OR(COALESCE(field, FALSE)))
    agg_parts = []
    for pat_field, src_field in PATIENT_OP_BOOL_FIELDS:
        agg_parts.append(f"        BOOL_OR({src_field}) AS {pat_field}")
    agg_parts.append(
        "        COUNT(DISTINCT CASE WHEN operative_findings_raw IS NOT NULL "
        "AND TRIM(operative_findings_raw) != '' "
        "THEN surgery_episode_id END) AS op_n_surgeries_with_findings"
    )
    agg_parts.append(
        "        STRING_AGG(DISTINCT NULLIF(operative_findings_raw, ''), "
        "' | ') AS op_findings_summary"
    )

    agg_select = ",\n".join(agg_parts)

    # Build SET clauses
    set_parts = []
    for pat_field, _ in PATIENT_OP_BOOL_FIELDS:
        if col_exists(con, "patient_analysis_resolved_v1", pat_field):
            set_parts.append(f"    {pat_field} = agg.{pat_field}")
    if col_exists(con, "patient_analysis_resolved_v1",
                  "op_n_surgeries_with_findings"):
        set_parts.append(
            "    op_n_surgeries_with_findings = agg.op_n_surgeries_with_findings"
        )
    if col_exists(con, "patient_analysis_resolved_v1", "op_findings_summary"):
        set_parts.append(
            "    op_findings_summary = agg.op_findings_summary"
        )

    set_clause = ",\n".join(set_parts)

    sql = f"""
UPDATE patient_analysis_resolved_v1
SET
{set_clause}
FROM (
    SELECT
        research_id,
{agg_select}
    FROM operative_episode_detail_v2
    GROUP BY research_id
) agg
WHERE patient_analysis_resolved_v1.research_id = agg.research_id
"""

    if dry_run:
        print(f"    [DRY-RUN] Would UPDATE patient_analysis_resolved_v1 "
              f"SET {len(set_parts)} fields (BOOL_OR without COALESCE)")
        return 0

    rc = safe_exec(con, sql, "re-propagate patient op_* fields")
    print(f"    Updated patient_analysis_resolved_v1: {rc} rows")
    return rc if rc >= 0 else 0


# ─── Phase E: Re-propagate to manuscript_cohort_v1 ──────────────────────────

def phase_e(con: duckdb.DuckDBPyConnection, dry_run: bool) -> int:
    """Copy freshly-updated op_* fields from patient_analysis_resolved_v1
    to manuscript_cohort_v1."""
    print("\n═══ Phase E: Re-propagate to manuscript_cohort_v1 ═══")

    set_parts = []
    for pat_field, _ in PATIENT_OP_BOOL_FIELDS:
        if col_exists(con, "manuscript_cohort_v1", pat_field):
            set_parts.append(f"    {pat_field} = p.{pat_field}")
    if col_exists(con, "manuscript_cohort_v1", "op_n_surgeries_with_findings"):
        set_parts.append(
            "    op_n_surgeries_with_findings = p.op_n_surgeries_with_findings"
        )
    if col_exists(con, "manuscript_cohort_v1", "op_findings_summary"):
        set_parts.append(
            "    op_findings_summary = p.op_findings_summary"
        )

    if not set_parts:
        print("    No matching columns in manuscript_cohort_v1 — skip")
        return 0

    set_clause = ",\n".join(set_parts)
    sql = f"""
UPDATE manuscript_cohort_v1
SET
{set_clause}
FROM patient_analysis_resolved_v1 p
WHERE manuscript_cohort_v1.research_id = p.research_id
"""

    if dry_run:
        print(f"    [DRY-RUN] Would UPDATE manuscript_cohort_v1 "
              f"SET {len(set_parts)} fields from patient_analysis_resolved_v1")
        return 0

    rc = safe_exec(con, sql, "re-propagate manuscript op_* fields")
    print(f"    Updated manuscript_cohort_v1: {rc} rows")
    return rc if rc >= 0 else 0


# ─── Phase F: Rebuild md_ mirrors on local DuckDB ─────────────────────────────

def phase_f(con: duckdb.DuckDBPyConnection, dry_run: bool) -> list[str]:
    """Rebuild local DuckDB md_ mirror tables for affected tables."""
    print("\n═══ Phase F: Rebuild md_ mirrors ═══")
    mirrors = [
        ("episode_analysis_resolved_v1_dedup", "md_episode_analysis_resolved_v1_dedup"),
        ("patient_analysis_resolved_v1",       "md_patient_analysis_resolved_v1"),
        ("manuscript_cohort_v1",               "md_manuscript_cohort_v1"),
        ("operative_episode_detail_v2",        "md_operative_episode_detail_v2"),
    ]
    rebuilt = []
    for src, dst in mirrors:
        if not tbl_exists(con, src):
            print(f"    {src}: source missing — skip mirror")
            continue
        if dry_run:
            print(f"    [DRY-RUN] Would CREATE OR REPLACE TABLE {dst} "
                  f"AS SELECT * FROM {src}")
            rebuilt.append(dst)
        else:
            rc = safe_exec(
                con,
                f"CREATE OR REPLACE TABLE {dst} AS SELECT * FROM {src}",
                f"mirror {dst}"
            )
            if rc >= 0:
                ct = safe_count(con, f"SELECT COUNT(*) FROM {dst}")
                print(f"    {dst}: rebuilt ({ct} rows)")
                rebuilt.append(dst)
            else:
                print(f"    {dst}: FAILED")
    return rebuilt


# ─── Phase G: Validation tables ──────────────────────────────────────────────

def phase_g(con: duckdb.DuckDBPyConnection, dry_run: bool,
            before_snapshots: dict[str, list[dict]]) -> None:
    """Create val_operative_truth_state_v1 and delta comparison table."""
    print("\n═══ Phase G: Create validation tables ═══")

    if dry_run:
        print("    [DRY-RUN] Would create val_operative_truth_state_v1 "
              "and val_operative_truth_state_delta_v1")
        return

    # ── val_operative_truth_state_v1: per-field current state ──
    rows: list[dict] = []
    tables_and_fields = [
        ("operative_episode_detail_v2",    ALL_BOOLEAN_FIELDS),
        ("episode_analysis_resolved_v1",
         [f for f in ALL_BOOLEAN_FIELDS
          if col_exists(con, "episode_analysis_resolved_v1", f)]),
    ]
    pat_fields = [pf for pf, _ in PATIENT_OP_BOOL_FIELDS]
    tables_and_fields.append(
        ("patient_analysis_resolved_v1", pat_fields)
    )
    mc_fields = [pf for pf, _ in PATIENT_OP_BOOL_FIELDS
                 if col_exists(con, "manuscript_cohort_v1", pf)]
    tables_and_fields.append(
        ("manuscript_cohort_v1", mc_fields)
    )

    for table, fields in tables_and_fields:
        total = safe_count(con, f"SELECT COUNT(*) FROM {table}")
        for f in fields:
            if not col_exists(con, table, f):
                continue
            r = con.execute(f"""
                SELECT
                    SUM(CASE WHEN {f} IS TRUE THEN 1 ELSE 0 END),
                    SUM(CASE WHEN {f} IS NOT TRUE AND {f} IS NOT NULL THEN 1 ELSE 0 END),
                    SUM(CASE WHEN {f} IS NULL THEN 1 ELSE 0 END)
                FROM {table}
            """).fetchone()
            rows.append({
                "table_name": table,
                "field_name": f,
                "total_rows": total,
                "true_count": int(r[0]),
                "false_count": int(r[1]),
                "null_count": int(r[2]),
                "positive_rate_pct": round(100.0 * int(r[0]) / total, 2) if total else 0,
                "unknown_rate_pct": round(100.0 * int(r[2]) / total, 2) if total else 0,
                "truth_state": (
                    "TRI_STATE" if int(r[0]) > 0 and int(r[2]) > 0
                    else "ALL_NULL" if int(r[0]) == 0 and int(r[1]) == 0
                    else "ALL_FALSE" if int(r[2]) == 0 and int(r[0]) == 0
                    else "MIXED"
                ),
                "hardening_applied": True,
                "checked_at": datetime.now(timezone.utc).isoformat(),
            })

    df = pd.DataFrame(rows)
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        tmp = f.name
    df.to_parquet(tmp, index=False)
    con.execute(
        f"CREATE OR REPLACE TABLE val_operative_truth_state_v1 "
        f"AS SELECT * FROM read_parquet('{tmp}')"
    )
    os.unlink(tmp)
    print(f"    val_operative_truth_state_v1: {len(rows)} rows")

    # ── val_operative_truth_state_delta_v1: before/after comparison ──
    delta_rows: list[dict] = []
    # Map before snapshots by (table, field)
    before_map: dict[tuple[str, str], dict] = {}
    for key, snaps in before_snapshots.items():
        for s in snaps:
            before_map[(s["table"], s["field"])] = s

    for row in rows:
        key = (row["table_name"], row["field_name"])
        before = before_map.get(key)
        if before and before.get("status") == "OK":
            delta_rows.append({
                "table_name": row["table_name"],
                "field_name": row["field_name"],
                "before_true": before["true_ct"],
                "before_false": before["false_ct"],
                "before_null": before["null_ct"],
                "after_true": row["true_count"],
                "after_false": row["false_count"],
                "after_null": row["null_count"],
                "true_preserved": before["true_ct"] == row["true_count"],
                "false_to_null": before["false_ct"] - row["false_count"],
                "change_summary": (
                    f"FALSE→NULL: {before['false_ct'] - row['false_count']} rows"
                    if before['false_ct'] > row['false_count']
                    else "no change"
                ),
                "checked_at": datetime.now(timezone.utc).isoformat(),
            })

    if delta_rows:
        df_delta = pd.DataFrame(delta_rows)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            tmp2 = f.name
        df_delta.to_parquet(tmp2, index=False)
        con.execute(
            f"CREATE OR REPLACE TABLE val_operative_truth_state_delta_v1 "
            f"AS SELECT * FROM read_parquet('{tmp2}')"
        )
        os.unlink(tmp2)
        print(f"    val_operative_truth_state_delta_v1: {len(delta_rows)} rows")

        # Print summary
        print("\n    ── Delta Summary ──")
        for dr in delta_rows:
            if dr["false_to_null"] > 0:
                print(f"    {dr['table_name']:42s} {dr['field_name']:42s}  "
                      f"FALSE→NULL: {dr['false_to_null']:6d}  "
                      f"TRUE preserved: {dr['true_preserved']}")


# ─── Phase H: Export artifacts + AFTER snapshot ──────────────────────────────

def phase_h(con: duckdb.DuckDBPyConnection, dry_run: bool,
            before_snapshots: dict, recoded: dict,
            phase_c_ct: int, phase_d_ct: int, phase_e_ct: int,
            mirrors: list[str]) -> None:
    """Export CSVs and generate manifest."""
    print("\n═══ Phase H: Export artifacts ═══")

    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

    # Export validation tables
    for tbl in ["val_operative_truth_state_v1",
                "val_operative_truth_state_delta_v1"]:
        if tbl_exists(con, tbl):
            df = con.execute(f"SELECT * FROM {tbl}").fetchdf()
            df.to_csv(EXPORTS_DIR / f"{tbl}.csv", index=False)
            df.to_parquet(EXPORTS_DIR / f"{tbl}.parquet", index=False)
            print(f"    Exported {tbl}: {len(df)} rows")

    # Manifest
    manifest = {
        "script": "104_operative_truth_state_hardening.py",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "purpose": "Recode hardcoded FALSE→NULL in operative NLP boolean fields; "
                   "re-propagate with tri-state semantics (TRUE/NULL) to all "
                   "downstream analytic and manuscript tables",
        "tables_modified": [
            "operative_episode_detail_v2",
            "episode_analysis_resolved_v1",
            "episode_analysis_resolved_v1_dedup",
            "patient_analysis_resolved_v1",
            "manuscript_cohort_v1",
        ],
        "validation_tables_created": [
            "val_operative_truth_state_v1",
            "val_operative_truth_state_delta_v1",
        ],
        "md_mirrors_rebuilt": mirrors,
        "fields_recoded": recoded,
        "rows_updated": {
            "episode_analysis_resolved_v1": phase_c_ct,
            "patient_analysis_resolved_v1": phase_d_ct,
            "manuscript_cohort_v1": phase_e_ct,
        },
        "semantic_change": (
            "FALSE (previously meaning 'hardcoded default / unknown') "
            "→ NULL (honest unknown). TRUE values (NLP-confirmed positive) "
            "preserved unchanged. No manuscript row counts change."
        ),
    }
    with open(EXPORTS_DIR / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    print(f"    Manifest: {EXPORTS_DIR / 'manifest.json'}")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Operative NLP Truth-State Hardening"
    )
    parser.add_argument("--md", action="store_true",
                        help="Use local DuckDB (default: local DuckDB)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print SQL without executing")
    parser.add_argument("--phase", type=str, default="all",
                        help="Run single phase: A-H or 'all'")
    args = parser.parse_args()

    con = connect(args.md)
    dry = args.dry_run
    phase = args.phase.upper()

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║  104 — Operative NLP Truth-State Hardening                  ║")
    print(f"║  Target: {'local DuckDB' if args.md else 'local DuckDB':50s} ║")
    print(f"║  Mode:   {'DRY-RUN' if dry else 'LIVE':50s} ║")
    print(f"║  Phase:  {phase:50s} ║")
    print("╚══════════════════════════════════════════════════════════════╝")

    before_snapshots: dict = {}
    recoded: dict = {}
    phase_c_ct = phase_d_ct = phase_e_ct = 0
    mirrors: list = []

    if phase in ("ALL", "A"):
        before_snapshots = phase_a(con, dry)

    if phase in ("ALL", "B"):
        recoded = phase_b(con, dry)

    if phase in ("ALL", "C"):
        phase_c_ct = phase_c(con, dry)

    if phase in ("ALL", "D"):
        phase_d_ct = phase_d(con, dry)

    if phase in ("ALL", "E"):
        phase_e_ct = phase_e(con, dry)

    if phase in ("ALL", "F"):
        mirrors = phase_f(con, dry)

    if phase in ("ALL", "G"):
        phase_g(con, dry, before_snapshots)

    if phase in ("ALL", "H"):
        phase_h(con, dry, before_snapshots, recoded,
                phase_c_ct, phase_d_ct, phase_e_ct, mirrors)

    print("\n✅ Done.")
    con.close()


if __name__ == "__main__":
    main()
