#!/usr/bin/env python3
"""
96_episode_downstream_repair.py — Fix episode_id propagation + v3 linkage routing

Addresses three critical defects identified in multi_surgery_truth_snapshot:
  1. operative_episode_detail_v2: all surgery_episode_id=1 (broken)
  2. episode_analysis_resolved_v1_dedup: all surgery_episode_id=1 (broken)
  3. v3 linkage tables have surgery_episode_id column but ALL values=1

Approach:
  - Uses temporal matching from deduplicated tumor_episode_master_v2 to build
    canonical (research_id, surgery_date) → surgery_episode_id mapping
  - Preserves all existing columns; writes provenance audit table
  - Non-regression: single-surgery patients remain ep_id=1 (no change)

Schema Notes (from live inspection 2026-03-15):
  - operative_episode_detail_v2.surgery_date_native = DATE (reliable)
  - operative_episode_detail_v2.resolved_surgery_date = VARCHAR (unreliable)
  - episode_analysis_resolved_v1_dedup.surgery_date = DATE
  - tumor_episode_master_v2.surgery_date = DATE
  - surgery_pathology_linkage_v3 uses `surg_date` (NOT surgery_date!)
  - pathology_rai_linkage_v3 uses `surgery_date` + `rai_date` (both DATE)
  - preop_surgery_linkage_v3 uses `surgery_date` (DATE)
  - All v3 linkage tables already have surgery_episode_id BIGINT column

Usage:
    .venv/bin/python scripts/96_episode_downstream_repair.py --md [--dry-run]
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    import duckdb
except ImportError:
    sys.exit("duckdb not installed")

NOW = datetime.datetime.now()
TIMESTAMP = NOW.strftime("%Y%m%d_%H%M")
DATE_TAG = NOW.strftime("%Y%m%d")
DOCS_DIR = ROOT / "docs"
EXPORT_DIR = ROOT / f"exports/episode_downstream_repair_{TIMESTAMP}"


# ── helpers ──────────────────────────────────────────────────────────────

def section(title: str):
    print(f"\n{'='*60}\n  {title}\n{'='*60}")


def get_connection(args):
    tok = os.environ.get("MOTHERDUCK_TOKEN", "")
    if not tok:
        try:
            import toml
            for p in [ROOT / ".streamlit" / "secrets.toml",
                      pathlib.Path.home() / ".streamlit" / "secrets.toml"]:
                if p.exists():
                    tok = toml.load(str(p)).get("MOTHERDUCK_TOKEN", "")
                    if tok:
                        break
        except ImportError:
            pass
    if not tok:
        sys.exit("MOTHERDUCK_TOKEN not found")
    os.environ["MOTHERDUCK_TOKEN"] = tok
    db = "thyroid_research_2026"
    con = duckdb.connect(f"md:{db}?motherduck_token={tok}")
    print(f"Connected to md:{db}")
    return con


def q1(con, sql, default=None):
    try:
        r = con.execute(sql).fetchone()
        return r[0] if r else default
    except Exception as e:
        print(f"  WARN q1: {e}")
        return default


def qall(con, sql):
    try:
        return con.execute(sql).fetchall()
    except Exception as e:
        print(f"  WARN qall: {e}")
        return []


# ── canonical map ────────────────────────────────────────────────────────

def build_canonical_map(con):
    """
    Build unique (research_id, surgery_date) → surgery_episode_id from
    tumor_episode_master_v2.  Multi-tumor rows for same (rid, date, ep_id)
    are collapsed via ROW_NUMBER().
    """
    section("BUILD CANONICAL EP-DATE MAP")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _ep_date_map AS
        SELECT research_id, surgery_episode_id, surgery_date
        FROM (
            SELECT
                research_id,
                surgery_episode_id,
                surgery_date,
                ROW_NUMBER() OVER (
                    PARTITION BY research_id, surgery_date
                    ORDER BY tumor_ordinal
                ) AS rn
            FROM tumor_episode_master_v2
            WHERE surgery_date IS NOT NULL
        )
        WHERE rn = 1
    """)
    n = q1(con, "SELECT COUNT(*) FROM _ep_date_map", 0)
    ep_max = q1(con, "SELECT MAX(surgery_episode_id) FROM _ep_date_map", 0)
    n_multi = q1(con, """
        SELECT COUNT(DISTINCT research_id) FROM _ep_date_map
        WHERE research_id IN (
            SELECT research_id FROM _ep_date_map
            GROUP BY research_id HAVING COUNT(*) > 1
        )
    """, 0)
    print(f"  Map rows: {n}, max ep_id: {ep_max}, multi-surg patients: {n_multi}")
    return n


# ── metrics capture (reusable for before/after) ─────────────────────────

def capture_metrics(con, label: str) -> dict:
    section(f"{label} — Metrics")
    m = {}

    for tbl, short in [
        ("operative_episode_detail_v2", "oed"),
        ("episode_analysis_resolved_v1_dedup", "eard"),
    ]:
        dist = qall(con, f"""
            SELECT surgery_episode_id, COUNT(*)
            FROM {tbl} GROUP BY 1 ORDER BY 1
        """)
        m[f"{short}_dist"] = {int(r[0]): r[1] for r in dist}
        m[f"{short}_total"] = sum(r[1] for r in dist)
        m[f"{short}_non1"] = sum(r[1] for r in dist if r[0] != 1)
        ms_pts = q1(con, f"""
            SELECT COUNT(DISTINCT t.research_id)
            FROM {tbl} t
            WHERE t.research_id IN (
                SELECT research_id FROM _ep_date_map
                GROUP BY research_id HAVING COUNT(*) > 1
            ) AND t.surgery_episode_id > 1
        """, 0)
        m[f"{short}_ms_pts_ep_gt1"] = ms_pts
        print(f"  {tbl}: total={m[f'{short}_total']}, "
              f"non-ep1={m[f'{short}_non1']}, ms-pts-with-ep>1={ms_pts}")

    for tbl, date_col in [
        ("surgery_pathology_linkage_v3", "surg_date"),
        ("pathology_rai_linkage_v3", "surgery_date"),
        ("preop_surgery_linkage_v3", "surgery_date"),
    ]:
        key = tbl.replace("_v3", "")
        dist_v = qall(con, f"""
            SELECT surgery_episode_id, COUNT(*)
            FROM {tbl} GROUP BY 1 ORDER BY 1
        """)
        m[f"{key}_dist"] = {int(r[0]): r[1] for r in dist_v}
        m[f"{key}_non1"] = sum(r[1] for r in dist_v if r[0] != 1)
        print(f"  {tbl}: non-ep1={m[f'{key}_non1']}, "
              f"dist={[(r[0], r[1]) for r in dist_v]}")

    return m


# ── FIX: operative_episode_detail_v2 ────────────────────────────────────

def fix_operative(con) -> dict:
    section("FIX: operative_episode_detail_v2")
    stats = {}

    # Build fix mapping via surgery_date_native (DATE type, reliable)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _oed_fix AS
        SELECT
            o.research_id,
            o.surgery_date_native,
            o.surgery_episode_id AS old_ep_id,
            COALESCE(m.surgery_episode_id, o.surgery_episode_id) AS new_ep_id,
            CASE
                WHEN m.surgery_episode_id IS NOT NULL
                     AND m.surgery_episode_id != o.surgery_episode_id
                    THEN 'repaired'
                WHEN m.surgery_episode_id IS NOT NULL
                     AND m.surgery_episode_id = o.surgery_episode_id
                    THEN 'already_correct'
                ELSE 'no_date_match'
            END AS repair_status
        FROM operative_episode_detail_v2 o
        LEFT JOIN _ep_date_map m
          ON o.research_id = m.research_id
          AND o.surgery_date_native = m.surgery_date
    """)

    status_dist = qall(con, """
        SELECT repair_status, COUNT(*) FROM _oed_fix GROUP BY 1 ORDER BY 1
    """)
    stats["fix_map"] = {r[0]: r[1] for r in status_dist}
    print(f"  Fix map: {stats['fix_map']}")

    # Apply UPDATE
    con.execute("""
        UPDATE operative_episode_detail_v2 AS o
        SET surgery_episode_id = f.new_ep_id
        FROM _oed_fix f
        WHERE o.research_id = f.research_id
          AND o.surgery_date_native = f.surgery_date_native
          AND f.repair_status = 'repaired'
    """)

    updated = q1(con, """
        SELECT COUNT(*) FROM operative_episode_detail_v2
        WHERE surgery_episode_id != 1
    """, 0)
    stats["rows_with_ep_gt1"] = updated
    print(f"  Rows with ep_id > 1 after fix: {updated}")

    no_match = q1(con, """
        SELECT COUNT(*) FROM _oed_fix WHERE repair_status = 'no_date_match'
    """, 0)
    stats["no_date_match"] = no_match
    if no_match > 0:
        print(f"  WARNING: {no_match} rows had no date match")
        samples = qall(con, """
            SELECT research_id, surgery_date_native
            FROM _oed_fix WHERE repair_status = 'no_date_match'
            LIMIT 5
        """)
        for s in samples:
            print(f"    rid={s[0]}, date={s[1]}")

    return stats


# ── FIX: episode_analysis_resolved_v1_dedup ──────────────────────────────

def fix_eard(con) -> dict:
    section("FIX: episode_analysis_resolved_v1_dedup")
    stats = {}

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _eard_fix AS
        SELECT
            e.research_id,
            e.surgery_date,
            e.surgery_episode_id AS old_ep_id,
            COALESCE(m.surgery_episode_id, e.surgery_episode_id) AS new_ep_id,
            CASE
                WHEN m.surgery_episode_id IS NOT NULL
                     AND m.surgery_episode_id != e.surgery_episode_id
                    THEN 'repaired'
                WHEN m.surgery_episode_id IS NOT NULL
                     AND m.surgery_episode_id = e.surgery_episode_id
                    THEN 'already_correct'
                ELSE 'no_date_match'
            END AS repair_status
        FROM episode_analysis_resolved_v1_dedup e
        LEFT JOIN _ep_date_map m
          ON e.research_id = m.research_id
          AND e.surgery_date = m.surgery_date
    """)

    status_dist = qall(con, """
        SELECT repair_status, COUNT(*) FROM _eard_fix GROUP BY 1 ORDER BY 1
    """)
    stats["fix_map"] = {r[0]: r[1] for r in status_dist}
    print(f"  Fix map: {stats['fix_map']}")

    con.execute("""
        UPDATE episode_analysis_resolved_v1_dedup AS e
        SET surgery_episode_id = f.new_ep_id
        FROM _eard_fix f
        WHERE e.research_id = f.research_id
          AND e.surgery_date = f.surgery_date
          AND f.repair_status = 'repaired'
    """)

    updated = q1(con, """
        SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup
        WHERE surgery_episode_id != 1
    """, 0)
    stats["rows_with_ep_gt1"] = updated
    print(f"  Rows with ep_id > 1 after fix: {updated}")

    no_match = q1(con, """
        SELECT COUNT(*) FROM _eard_fix WHERE repair_status = 'no_date_match'
    """, 0)
    stats["no_date_match"] = no_match
    if no_match > 0:
        print(f"  WARNING: {no_match} rows had no date match")

    return stats


# ── FIX: surgery_pathology_linkage_v3 (uses surg_date, NOT surgery_date) ─

def fix_surgery_pathology_linkage(con) -> dict:
    section("FIX: surgery_pathology_linkage_v3")
    stats = {}

    # This table uses `surg_date` (confirmed by schema check)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _spl_fix AS
        SELECT
            sp.research_id,
            sp.surg_date,
            sp.tumor_ordinal,
            sp.surgery_episode_id AS old_ep_id,
            COALESCE(m.surgery_episode_id, sp.surgery_episode_id) AS new_ep_id,
            CASE
                WHEN m.surgery_episode_id IS NOT NULL
                     AND m.surgery_episode_id != sp.surgery_episode_id
                    THEN 'repaired'
                WHEN m.surgery_episode_id IS NOT NULL
                     AND m.surgery_episode_id = sp.surgery_episode_id
                    THEN 'already_correct'
                ELSE 'no_date_match'
            END AS repair_status
        FROM surgery_pathology_linkage_v3 sp
        LEFT JOIN _ep_date_map m
          ON sp.research_id = m.research_id
          AND TRY_CAST(sp.surg_date AS DATE) = m.surgery_date
    """)

    status_dist = qall(con, """
        SELECT repair_status, COUNT(*) FROM _spl_fix GROUP BY 1 ORDER BY 1
    """)
    stats["fix_map"] = {r[0]: r[1] for r in status_dist}
    print(f"  Fix map: {stats['fix_map']}")

    # UPDATE using composite key (research_id, surg_date, tumor_ordinal)
    con.execute("""
        UPDATE surgery_pathology_linkage_v3 AS sp
        SET surgery_episode_id = f.new_ep_id
        FROM _spl_fix f
        WHERE sp.research_id = f.research_id
          AND sp.surg_date = f.surg_date
          AND COALESCE(CAST(sp.tumor_ordinal AS INTEGER), 0)
            = COALESCE(CAST(f.tumor_ordinal AS INTEGER), 0)
          AND f.repair_status = 'repaired'
    """)

    updated = q1(con, """
        SELECT COUNT(*) FROM surgery_pathology_linkage_v3
        WHERE surgery_episode_id != 1
    """, 0)
    stats["rows_with_ep_gt1"] = updated
    print(f"  Rows with ep_id > 1: {updated}")
    return stats


# ── FIX: pathology_rai_linkage_v3 ───────────────────────────────────────

def fix_pathology_rai_linkage(con) -> dict:
    section("FIX: pathology_rai_linkage_v3")
    stats = {}

    # RAI links to cancer surgery — find nearest preceding surgery episode
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _prl_fix AS
        WITH ranked AS (
            SELECT
                pr.research_id,
                pr.rai_date,
                pr.surgery_date  AS pr_surg_date,
                pr.surgery_episode_id AS old_ep_id,
                m.surgery_episode_id  AS candidate_ep_id,
                m.surgery_date        AS canonical_surg_date,
                DATE_DIFF('day', m.surgery_date, pr.rai_date) AS days_after,
                ROW_NUMBER() OVER (
                    PARTITION BY pr.research_id, pr.rai_date
                    ORDER BY
                        -- prefer surgery BEFORE rai_date
                        CASE WHEN DATE_DIFF('day', m.surgery_date, pr.rai_date) >= 0
                             THEN 0 ELSE 1 END,
                        ABS(DATE_DIFF('day', m.surgery_date, pr.rai_date))
                ) AS rn
            FROM pathology_rai_linkage_v3 pr
            LEFT JOIN _ep_date_map m
              ON pr.research_id = m.research_id
        )
        SELECT
            research_id,
            rai_date,
            pr_surg_date,
            old_ep_id,
            COALESCE(candidate_ep_id, old_ep_id) AS new_ep_id,
            canonical_surg_date,
            days_after,
            CASE
                WHEN candidate_ep_id IS NOT NULL
                     AND candidate_ep_id != old_ep_id THEN 'repaired'
                WHEN candidate_ep_id IS NOT NULL
                     AND candidate_ep_id = old_ep_id THEN 'already_correct'
                ELSE 'no_match'
            END AS repair_status
        FROM ranked
        WHERE rn = 1
    """)

    status_dist = qall(con, """
        SELECT repair_status, COUNT(*) FROM _prl_fix GROUP BY 1 ORDER BY 1
    """)
    stats["fix_map"] = {r[0]: r[1] for r in status_dist}
    print(f"  Fix map: {stats['fix_map']}")

    con.execute("""
        UPDATE pathology_rai_linkage_v3 AS pr
        SET surgery_episode_id = f.new_ep_id
        FROM _prl_fix f
        WHERE pr.research_id = f.research_id
          AND pr.rai_date = f.rai_date
          AND f.repair_status = 'repaired'
    """)

    updated = q1(con, """
        SELECT COUNT(*) FROM pathology_rai_linkage_v3
        WHERE surgery_episode_id != 1
    """, 0)
    stats["rows_with_ep_gt1"] = updated
    print(f"  Rows with ep_id > 1: {updated}")
    return stats


# ── FIX: preop_surgery_linkage_v3 ───────────────────────────────────────

def fix_preop_surgery_linkage(con) -> dict:
    section("FIX: preop_surgery_linkage_v3")
    stats = {}

    con.execute("""
        CREATE OR REPLACE TEMP TABLE _psl_fix AS
        SELECT
            ps.research_id,
            ps.surgery_date,
            ps.preop_date,
            ps.surgery_episode_id AS old_ep_id,
            COALESCE(m.surgery_episode_id, ps.surgery_episode_id) AS new_ep_id,
            CASE
                WHEN m.surgery_episode_id IS NOT NULL
                     AND m.surgery_episode_id != ps.surgery_episode_id
                    THEN 'repaired'
                WHEN m.surgery_episode_id IS NOT NULL
                     AND m.surgery_episode_id = ps.surgery_episode_id
                    THEN 'already_correct'
                ELSE 'no_date_match'
            END AS repair_status
        FROM preop_surgery_linkage_v3 ps
        LEFT JOIN _ep_date_map m
          ON ps.research_id = m.research_id
          AND ps.surgery_date = m.surgery_date
    """)

    status_dist = qall(con, """
        SELECT repair_status, COUNT(*) FROM _psl_fix GROUP BY 1 ORDER BY 1
    """)
    stats["fix_map"] = {r[0]: r[1] for r in status_dist}
    print(f"  Fix map: {stats['fix_map']}")

    con.execute("""
        UPDATE preop_surgery_linkage_v3 AS ps
        SET surgery_episode_id = f.new_ep_id
        FROM _psl_fix f
        WHERE ps.research_id = f.research_id
          AND ps.surgery_date = f.surgery_date
          AND ps.preop_date = f.preop_date
          AND f.repair_status = 'repaired'
    """)

    updated = q1(con, """
        SELECT COUNT(*) FROM preop_surgery_linkage_v3
        WHERE surgery_episode_id != 1
    """, 0)
    stats["rows_with_ep_gt1"] = updated
    print(f"  Rows with ep_id > 1: {updated}")
    return stats


# ── PROVENANCE AUDIT TABLE ──────────────────────────────────────────────

def write_provenance(con):
    section("PROVENANCE AUDIT TABLE")
    con.execute("""
        CREATE OR REPLACE TABLE episode_downstream_repair_audit_v1 AS
        SELECT 'operative_episode_detail_v2' AS target_table,
            repair_status, COUNT(*) AS row_count,
            CURRENT_TIMESTAMP AS repaired_at,
            'script_96' AS source_script
        FROM _oed_fix GROUP BY 1, 2
        UNION ALL
        SELECT 'episode_analysis_resolved_v1_dedup',
            repair_status, COUNT(*), CURRENT_TIMESTAMP, 'script_96'
        FROM _eard_fix GROUP BY 1, 2
        UNION ALL
        SELECT 'surgery_pathology_linkage_v3',
            repair_status, COUNT(*), CURRENT_TIMESTAMP, 'script_96'
        FROM _spl_fix GROUP BY 1, 2
        UNION ALL
        SELECT 'pathology_rai_linkage_v3',
            repair_status, COUNT(*), CURRENT_TIMESTAMP, 'script_96'
        FROM _prl_fix GROUP BY 1, 2
        UNION ALL
        SELECT 'preop_surgery_linkage_v3',
            repair_status, COUNT(*), CURRENT_TIMESTAMP, 'script_96'
        FROM _psl_fix GROUP BY 1, 2
    """)
    rows = qall(con, """
        SELECT * FROM episode_downstream_repair_audit_v1 ORDER BY 1, 2
    """)
    for r in rows:
        print(f"  {r[0]}: {r[1]} = {r[2]}")
    return rows


# ── NON-REGRESSION ──────────────────────────────────────────────────────

def non_regression(con) -> dict:
    section("NON-REGRESSION CHECKS")
    checks = {}

    # Single-surgery patients must all be ep_id=1
    for tbl, short in [
        ("operative_episode_detail_v2", "oed"),
        ("episode_analysis_resolved_v1_dedup", "eard"),
    ]:
        bad = q1(con, f"""
            WITH single AS (
                SELECT research_id FROM _ep_date_map
                GROUP BY research_id HAVING COUNT(*) = 1
            )
            SELECT COUNT(*) FROM {tbl} t
            JOIN single s ON t.research_id = s.research_id
            WHERE t.surgery_episode_id != 1
        """, 0)
        key = f"single_surg_ep1_{short}"
        status = "PASS" if bad == 0 else f"FAIL ({bad})"
        checks[key] = status
        print(f"  {tbl}: single-surg all ep=1 → {status}")

    # Multi-surgery patients should have >1 distinct ep_id
    for tbl, short in [
        ("operative_episode_detail_v2", "oed"),
        ("episode_analysis_resolved_v1_dedup", "eard"),
    ]:
        multi_distinct = q1(con, f"""
            WITH multi AS (
                SELECT research_id FROM _ep_date_map
                GROUP BY research_id HAVING COUNT(*) > 1
            )
            SELECT COUNT(DISTINCT t.surgery_episode_id)
            FROM {tbl} t
            JOIN multi m ON t.research_id = m.research_id
        """, 0)
        checks[f"multi_surg_distinct_ep_{short}"] = multi_distinct
        print(f"  {tbl}: multi-surg distinct ep_ids → {multi_distinct}")

    # v3 linkage tables
    for tbl in [
        "surgery_pathology_linkage_v3",
        "pathology_rai_linkage_v3",
        "preop_surgery_linkage_v3",
    ]:
        distinct = q1(con, f"""
            SELECT COUNT(DISTINCT surgery_episode_id) FROM {tbl}
        """, 0)
        sn = tbl.replace("_linkage_v3", "")
        checks[f"v3_{sn}_distinct_ep"] = distinct
        print(f"  {tbl}: distinct ep_ids → {distinct}")

    return checks


# ── REPORT GENERATION ───────────────────────────────────────────────────

def generate_report(before, after, fix_stats, provenance, nonreg) -> str:
    lines = []
    a = lines.append

    a(f"# Episode Downstream Repair Report — {DATE_TAG}")
    a("")
    a(f"**Generated**: {TIMESTAMP}")
    a(f"**Script**: `scripts/96_episode_downstream_repair.py`")
    a(f"**Target**: MotherDuck `thyroid_research_2026` (prod)")
    a("")

    a("## Problem")
    a("")
    a("Multi-surgery audit (script 97) found all downstream tables had")
    a("`surgery_episode_id=1` for every row, despite `tumor_episode_master_v2`")
    a("correctly tracking 761 multi-surgery patients (1,576 episodes).")
    a("")

    a("## Repair Method")
    a("")
    a("Temporal matching: each rows surgery date is matched to the canonical")
    a("`(research_id, surgery_date) → surgery_episode_id` mapping from")
    a("`tumor_episode_master_v2` (deduplicated via `ROW_NUMBER()`).")
    a("")

    a("## Before / After")
    a("")
    a("### operative_episode_detail_v2")
    a("")
    a("| Metric | Before | After |")
    a("|--------|--------|-------|")
    a(f"| Total rows | {before.get('oed_total', '?')} | {after.get('oed_total', '?')} |")
    a(f"| Rows with ep_id > 1 | {before.get('oed_non1', 0)} | {after.get('oed_non1', '?')} |")
    a(f"| Multi-surg pts with ep>1 | {before.get('oed_ms_pts_ep_gt1', 0)} | {after.get('oed_ms_pts_ep_gt1', '?')} |")
    a("")
    a("ep_id distribution after:")
    a("")
    a("| ep_id | Count |")
    a("|-------|-------|")
    for k in sorted(after.get("oed_dist", {})):
        a(f"| {k} | {after['oed_dist'][k]} |")
    a("")

    a("### episode_analysis_resolved_v1_dedup")
    a("")
    a("| Metric | Before | After |")
    a("|--------|--------|-------|")
    a(f"| Total rows | {before.get('eard_total', '?')} | {after.get('eard_total', '?')} |")
    a(f"| Rows with ep_id > 1 | {before.get('eard_non1', 0)} | {after.get('eard_non1', '?')} |")
    a(f"| Multi-surg pts with ep>1 | {before.get('eard_ms_pts_ep_gt1', 0)} | {after.get('eard_ms_pts_ep_gt1', '?')} |")
    a("")
    a("ep_id distribution after:")
    a("")
    a("| ep_id | Count |")
    a("|-------|-------|")
    for k in sorted(after.get("eard_dist", {})):
        a(f"| {k} | {after['eard_dist'][k]} |")
    a("")

    a("### v3 Linkage Tables")
    a("")
    a("| Table | Non-ep1 Before | Non-ep1 After |")
    a("|-------|---------------|---------------|")
    for key_prefix, tbl in [
        ("surgery_pathology_linkage", "surgery_pathology_linkage_v3"),
        ("pathology_rai_linkage", "pathology_rai_linkage_v3"),
        ("preop_surgery_linkage", "preop_surgery_linkage_v3"),
    ]:
        b = before.get(f"{key_prefix}_non1", 0)
        af = after.get(f"{key_prefix}_non1", "?")
        a(f"| {tbl} | {b} | {af} |")
    a("")

    a("## Fix Statistics")
    a("")
    for tbl_name, st in fix_stats.items():
        a(f"### {tbl_name}")
        fm = st.get("fix_map", {})
        a("| Status | Count |")
        a("|--------|-------|")
        for status, cnt in sorted(fm.items()):
            a(f"| {status} | {cnt} |")
        nm = st.get("no_date_match", 0)
        if nm > 0:
            a(f"\n> ⚠️ {nm} rows had no date match in canonical map")
        a("")

    a("## Non-Regression")
    a("")
    a("| Check | Result |")
    a("|-------|--------|")
    for k, v in sorted(nonreg.items()):
        a(f"| {k} | {v} |")
    a("")

    a("## Provenance")
    a("")
    a("All repairs logged in `episode_downstream_repair_audit_v1`.")
    a("")
    a("| Target Table | Status | Count |")
    a("|-------------|--------|-------|")
    for r in (provenance or []):
        a(f"| {r[0]} | {r[1]} | {r[2]} |")
    a("")

    a("## MotherDuck Objects Modified")
    a("")
    a("| Table | Action |")
    a("|-------|--------|")
    a("| operative_episode_detail_v2 | UPDATE surgery_episode_id |")
    a("| episode_analysis_resolved_v1_dedup | UPDATE surgery_episode_id |")
    a("| surgery_pathology_linkage_v3 | UPDATE surgery_episode_id |")
    a("| pathology_rai_linkage_v3 | UPDATE surgery_episode_id |")
    a("| preop_surgery_linkage_v3 | UPDATE surgery_episode_id |")
    a("| episode_downstream_repair_audit_v1 | CREATE (provenance) |")
    a("")

    return "\n".join(lines)


# ── MAIN ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Episode downstream repair")
    parser.add_argument("--md", action="store_true", default=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print(f"Episode Downstream Repair — {TIMESTAMP}")
    print(f"  Dry-run: {args.dry_run}")

    con = get_connection(args)
    build_canonical_map(con)

    before = capture_metrics(con, "BEFORE")

    if args.dry_run:
        section("DRY-RUN — no changes made")
        con.close()
        return

    # Execute all repairs
    fix_stats = {}
    fix_stats["operative_episode_detail_v2"] = fix_operative(con)
    fix_stats["episode_analysis_resolved_v1_dedup"] = fix_eard(con)
    fix_stats["surgery_pathology_linkage_v3"] = fix_surgery_pathology_linkage(con)
    fix_stats["pathology_rai_linkage_v3"] = fix_pathology_rai_linkage(con)
    fix_stats["preop_surgery_linkage_v3"] = fix_preop_surgery_linkage(con)

    # Provenance
    provenance = write_provenance(con)

    # After metrics
    after = capture_metrics(con, "AFTER")

    # Non-regression
    nonreg = non_regression(con)

    # Report
    section("GENERATE REPORT")
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    report = generate_report(before, after, fix_stats, provenance, nonreg)
    report_path = DOCS_DIR / f"episode_downstream_repair_{DATE_TAG}.md"
    report_path.write_text(report)
    print(f"  Report: {report_path}")

    # JSON metrics
    all_metrics = {
        "before": before,
        "after": after,
        "fix_stats": fix_stats,
        "nonreg": nonreg,
        "timestamp": TIMESTAMP,
    }
    json_path = EXPORT_DIR / "repair_metrics.json"
    with open(json_path, "w") as f:
        json.dump(all_metrics, f, indent=2, default=str)
    print(f"  Metrics: {json_path}")

    con.close()
    section("DONE — All downstream episode_id repairs complete")


if __name__ == "__main__":
    main()
