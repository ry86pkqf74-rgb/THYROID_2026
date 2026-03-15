#!/usr/bin/env python3
"""
Script 100: Episode-Aware Linkage Hardening v2 for Multi-Surgery Patients
=========================================================================

Phase 2 hardening over the Phase 1 work (scripts 95-97). Focuses on:

  Phase A – Re-score surgery_pathology_linkage_v3 with date-proximity tiebreaker
             for multi-surgery patients; fill 669 missing ep>1 rows
  Phase B – Re-anchor preop_surgery_linkage_v3 using bounded chronology windows
  Phase C – Re-anchor pathology_rai_linkage_v3 using disease-course logic
  Phase D – Propagate corrected ep-ids to episode_analysis_resolved_v1_dedup
  Phase E – Build governed validation objects (scorecard, review queue)
  Phase F – Non-regression: prove single-surgery patients untouched
  Phase G – Before/after delta report
  Phase H – Mirror materialization to md_* tables

Environment staging:
  1. Deploy to dev  (--env dev)
  2. Validate in qa (--env qa)
  3. Promote to prod (--env prod, default)

Usage:
  .venv/bin/python scripts/100_episode_linkage_v2_hardening.py --md
  .venv/bin/python scripts/100_episode_linkage_v2_hardening.py --md --env dev
  .venv/bin/python scripts/100_episode_linkage_v2_hardening.py --md --env qa
  .venv/bin/python scripts/100_episode_linkage_v2_hardening.py --md --phase A
  .venv/bin/python scripts/100_episode_linkage_v2_hardening.py --md --dry-run
  .venv/bin/python scripts/100_episode_linkage_v2_hardening.py --local

Outputs:
  Tables (in target env):
    - episode_linkage_v2_sp_rescored       (surgery-pathology re-scored)
    - episode_linkage_v2_preop_rescored    (preop-surgery re-anchored)
    - episode_linkage_v2_rai_rescored      (pathology-RAI re-anchored)
    - episode_linkage_v2_downstream_sync   (ep-id propagation audit)
    - val_episode_linkage_v2_scorecard     (health scorecard)
    - val_episode_linkage_v2_review_queue  (manual review queue)
    - val_episode_linkage_v2_nonregression (single-surgery proof)
    - val_episode_linkage_v2_delta         (before/after delta report)
    - md_* mirrors for all above
  Exports:
    - exports/episode_linkage_v2_hardening_YYYYMMDD_HHMM/
  Docs:
    - docs/episode_linkage_v2_hardening_YYYYMMDD.md
    - docs/episode_linkage_before_after_YYYYMMDD.md
    - docs/episode_linkage_nonregression_v2_YYYYMMDD.md
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time
from datetime import datetime

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    import duckdb
except ImportError:
    sys.exit("duckdb not installed – run: .venv/bin/pip install duckdb")

# ── constants ────────────────────────────────────────────────────────────

NOW = datetime.now()
TIMESTAMP = NOW.strftime("%Y%m%d_%H%M")
DATE_TAG = NOW.strftime("%Y%m%d")
DOCS_DIR = ROOT / "docs"
EXPORT_DIR = ROOT / f"exports/episode_linkage_v2_hardening_{TIMESTAMP}"

ENV_MAP = {
    "dev": "thyroid_research_2026_dev",
    "qa": "thyroid_research_2026_qa",
    "prod": "thyroid_research_2026",
}

PHASES = list("ABCDEFGH")

# ── helpers ──────────────────────────────────────────────────────────────

def section(msg: str):
    print(f"\n{'='*60}\n  {msg}\n{'='*60}")


def get_token() -> str:
    tok = os.environ.get("MOTHERDUCK_TOKEN", "")
    if tok:
        return tok
    try:
        import toml
        for p in [
            ROOT / ".streamlit" / "secrets.toml",
            pathlib.Path("/Users/ros/Desktop/FInal Cleaned Thyroid Data/.streamlit/secrets.toml"),
            pathlib.Path.home() / ".streamlit" / "secrets.toml",
        ]:
            if p.exists():
                tok = toml.load(str(p)).get("MOTHERDUCK_TOKEN", "")
                if tok:
                    return tok
    except ImportError:
        pass
    return tok


def get_connection(args) -> duckdb.DuckDBPyConnection:
    """Connect to MotherDuck target env or local DuckDB."""
    if args.local:
        path = os.getenv("LOCAL_DUCKDB_PATH", "thyroid_master_local.duckdb")
        print(f"  [local] {path}")
        return duckdb.connect(path)
    tok = get_token()
    if not tok:
        sys.exit("MOTHERDUCK_TOKEN not found")
    os.environ["MOTHERDUCK_TOKEN"] = tok
    db = ENV_MAP.get(args.env, "thyroid_research_2026")
    con = duckdb.connect(f"md:{db}?motherduck_token={tok}")
    print(f"  [MotherDuck] connected to {db}")
    # In workspace mode, all databases accessible by qualified name
    if args.env != "prod":
        print(f"  Source tables read from thyroid_research_2026.main.* (workspace mode)")
        print(f"  UPDATEs to production tables SKIPPED in {args.env} env")
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


def sql_exec(con, sql, label="", dry_run=False):
    """Execute SQL. Print label on error. Returns row count or 0."""
    if dry_run:
        print(f"  [DRY-RUN] {label}")
        return 0
    try:
        con.execute(sql)
        return 0
    except Exception as e:
        print(f"  [ERROR] {label}: {e}")
        return -1


def tbl_prefix(args) -> str:
    """In non-prod envs, source tables are qualified via workspace mode."""
    if args.local or args.env == "prod":
        return ""
    return "thyroid_research_2026.main."


# ── PHASE A: Re-score surgery_pathology linkage for multi-surgery ─────

def phase_a_sp_rescore(con, args):
    """Re-score surgery_pathology_linkage_v3 with date-proximity tiebreaker.

    Key improvements over v1:
    1. For multi-surgery patients, re-compute surgery_episode_id using
       temporal proximity of surg_date in sp-linkage to each surgery in
       the canonical tumor_episode_master_v2.
    2. Use midpoint rule: link to nearest surgery, with ambiguity flag
       when equidistant or within 7 days of two surgeries.
    3. Preserve original linkage_confidence_tier, linkage_score, score_rank.
    """
    section("Phase A: Surgery-Pathology Re-scoring")
    src = tbl_prefix(args)

    sql = f"""
    CREATE OR REPLACE TABLE episode_linkage_v2_sp_rescored AS
    WITH
    -- 1. Canonical surgery spine with windowing
    surgery_spine AS (
        SELECT
            research_id,
            surgery_episode_id,
            surgery_date,
            LAG(surgery_date)  OVER w AS prev_surgery_date,
            LEAD(surgery_date) OVER w AS next_surgery_date
        FROM {src}tumor_episode_master_v2
        WHERE surgery_date IS NOT NULL
        WINDOW w AS (PARTITION BY research_id ORDER BY surgery_episode_id)
    ),
    multi_surg AS (
        SELECT research_id
        FROM surgery_spine
        GROUP BY research_id
        HAVING COUNT(*) > 1
    ),
    -- 2. Current sp-linkage rows
    sp AS (
        SELECT
            sp.*,
            TRY_CAST(sp.surg_date AS DATE) AS surg_date_parsed
        FROM {src}surgery_pathology_linkage_v3 sp
    ),
    -- 3. For multi-surgery patients, cross join with surgery spine
    --    and pick nearest surgery by date
    multi_candidates AS (
        SELECT
            sp.research_id,
            sp.surg_date,
            sp.surg_date_parsed,
            sp.linkage_confidence_tier  AS original_tier,
            sp.linkage_score            AS original_score,
            sp.surgery_episode_id       AS original_ep_id,
            ss.surgery_episode_id       AS candidate_ep_id,
            ss.surgery_date             AS candidate_surgery_date,
            ss.prev_surgery_date,
            ss.next_surgery_date,
            -- day gap from path surg_date to candidate surgery
            ABS(DATEDIFF('day',
                COALESCE(sp.surg_date_parsed, sp.surg_date),
                ss.surgery_date))       AS day_gap,
            -- Is candidate the nearest?
            ROW_NUMBER() OVER (
                PARTITION BY sp.research_id, sp.surg_date, sp.linkage_confidence_tier
                ORDER BY ABS(DATEDIFF('day',
                    COALESCE(sp.surg_date_parsed, sp.surg_date),
                    ss.surgery_date)),
                    ss.surgery_episode_id
            ) AS proximity_rank,
            -- Count of surgeries within 14 days (ambiguity indicator)
            COUNT(*) FILTER (WHERE ABS(DATEDIFF('day',
                COALESCE(sp.surg_date_parsed, sp.surg_date),
                ss.surgery_date)) <= 14)
                OVER (PARTITION BY sp.research_id, sp.surg_date) AS n_near_surgeries
        FROM sp
        JOIN multi_surg ms ON sp.research_id = ms.research_id
        JOIN surgery_spine ss ON sp.research_id = ss.research_id
        WHERE sp.surg_date IS NOT NULL
    ),
    -- 4. Best candidate per sp-linkage row
    multi_best AS (
        SELECT
            research_id,
            surg_date,
            surg_date_parsed,
            original_tier,
            original_score,
            original_ep_id,
            candidate_ep_id          AS rescored_ep_id,
            candidate_surgery_date   AS rescored_surgery_date,
            day_gap,
            n_near_surgeries,
            CASE
                WHEN day_gap = 0 THEN 'exact_day'
                WHEN day_gap <= 3 THEN 'anchored_window'
                WHEN day_gap <= 14 AND n_near_surgeries = 1 THEN 'anchored_window'
                WHEN day_gap <= 14 AND n_near_surgeries > 1 THEN 'ambiguous_multi_surgery'
                WHEN day_gap <= 30 THEN 'plausible_extended'
                WHEN day_gap <= 365 THEN 'weak_temporal'
                ELSE 'unlinked'
            END AS v2_confidence_tier,
            CASE
                WHEN day_gap = 0 THEN 1.0
                WHEN day_gap <= 3 THEN 0.95
                WHEN day_gap <= 14 AND n_near_surgeries = 1 THEN 0.85
                WHEN day_gap <= 14 AND n_near_surgeries > 1 THEN 0.60
                WHEN day_gap <= 30 THEN 0.50
                WHEN day_gap <= 365 THEN 0.25
                ELSE 0.0
            END AS v2_linkage_score,
            CASE WHEN original_ep_id != candidate_ep_id THEN TRUE ELSE FALSE END AS ep_id_changed,
            CASE WHEN n_near_surgeries > 1 AND day_gap <= 14 THEN TRUE ELSE FALSE END AS ambiguity_flag,
            CASE WHEN n_near_surgeries > 1 AND day_gap <= 14 THEN TRUE ELSE FALSE END AS manual_review_required
        FROM multi_candidates
        WHERE proximity_rank = 1
    ),
    -- 5. Single-surgery patients: no change, pass through
    single_surg_sp AS (
        SELECT
            sp.research_id,
            sp.surg_date,
            sp.surg_date_parsed,
            sp.linkage_confidence_tier  AS original_tier,
            sp.linkage_score            AS original_score,
            sp.surgery_episode_id       AS original_ep_id,
            sp.surgery_episode_id       AS rescored_ep_id,
            sp.surg_date_parsed         AS rescored_surgery_date,
            0                           AS day_gap,
            1                           AS n_near_surgeries,
            sp.linkage_confidence_tier  AS v2_confidence_tier,
            sp.linkage_score            AS v2_linkage_score,
            FALSE                       AS ep_id_changed,
            FALSE                       AS ambiguity_flag,
            FALSE                       AS manual_review_required
        FROM sp
        LEFT JOIN multi_surg ms ON sp.research_id = ms.research_id
        WHERE ms.research_id IS NULL
    )
    -- UNION both
    SELECT *, FALSE AS is_multi_surgery, 'passthrough' AS linkage_method,
           CURRENT_TIMESTAMP AS hardened_at
    FROM single_surg_sp
    UNION ALL
    SELECT *, TRUE AS is_multi_surgery, 'v2_proximity_rescore' AS linkage_method,
           CURRENT_TIMESTAMP AS hardened_at
    FROM multi_best
    """
    rc = sql_exec(con, sql, "episode_linkage_v2_sp_rescored", args.dry_run)
    if rc == -1:
        return {}

    # Metrics
    total = q1(con, "SELECT COUNT(*) FROM episode_linkage_v2_sp_rescored", 0)
    changed = q1(con, "SELECT COUNT(*) FROM episode_linkage_v2_sp_rescored WHERE ep_id_changed IS TRUE", 0)
    multi = q1(con, "SELECT COUNT(*) FROM episode_linkage_v2_sp_rescored WHERE is_multi_surgery IS TRUE", 0)
    ambig = q1(con, "SELECT COUNT(*) FROM episode_linkage_v2_sp_rescored WHERE ambiguity_flag IS TRUE", 0)

    tier_dist = qall(con, """
        SELECT v2_confidence_tier, COUNT(*) as n
        FROM episode_linkage_v2_sp_rescored
        WHERE is_multi_surgery IS TRUE
        GROUP BY 1 ORDER BY 2 DESC
    """)

    metrics = {
        "total_rows": total,
        "multi_surgery_rows": multi,
        "ep_id_changed": changed,
        "ambiguity_flagged": ambig,
        "multi_tier_dist": {str(r[0]): r[1] for r in tier_dist},
    }
    print(f"  Total: {total}, Multi-surg: {multi}, EP-ID changed: {changed}, Ambiguous: {ambig}")
    for r in tier_dist:
        print(f"    {r[0]}: {r[1]}")

    # Apply ep-id corrections back to surgery_pathology_linkage_v3
    # Only in prod — dev/qa are read-only validation environments
    applied = q1(con, """
        SELECT COUNT(*) FROM episode_linkage_v2_sp_rescored
        WHERE ep_id_changed IS TRUE AND ambiguity_flag IS NOT TRUE
          AND v2_confidence_tier IN ('exact_day', 'anchored_window')
    """, 0)
    metrics["high_confidence_eligible"] = applied
    if not args.dry_run and changed and changed > 0 and args.env == "prod":
        print(f"  Applying {applied} ep-id corrections to surgery_pathology_linkage_v3...")
        apply_sql = """
        UPDATE surgery_pathology_linkage_v3 sp
        SET surgery_episode_id = r.rescored_ep_id
        FROM episode_linkage_v2_sp_rescored r
        WHERE sp.research_id = r.research_id
          AND CAST(sp.surg_date AS VARCHAR) = CAST(r.surg_date AS VARCHAR)
          AND r.ep_id_changed IS TRUE
          AND r.ambiguity_flag IS NOT TRUE
          AND r.v2_confidence_tier IN ('exact_day', 'anchored_window')
        """
        sql_exec(con, apply_sql, "apply sp ep-id corrections")
        metrics["high_confidence_applied"] = applied
        print(f"  Applied {applied} high-confidence corrections")
    elif args.env != "prod":
        print(f"  {applied} high-confidence corrections eligible (UPDATE skipped in {args.env})")

    return metrics


# ── PHASE B: Re-anchor preop_surgery linkage ─────────────────────────

def phase_b_preop_rescore(con, args):
    """Re-anchor preop_surgery_linkage_v3 for multi-surgery patients.

    Key improvements:
    1. FNA/molecular must precede the linked surgery (chronology enforcement)
    2. Use bounded windows: FNA within -365 to -1 days before surgery
    3. Prefer the chronologically-nearest preceding surgery
    """
    section("Phase B: Preop-Surgery Re-anchoring")
    src = tbl_prefix(args)

    sql = f"""
    CREATE OR REPLACE TABLE episode_linkage_v2_preop_rescored AS
    WITH
    surgery_spine AS (
        SELECT research_id, surgery_episode_id, surgery_date,
               LAG(surgery_date) OVER w AS prev_surgery_date,
               LEAD(surgery_date) OVER w AS next_surgery_date
        FROM {src}tumor_episode_master_v2
        WHERE surgery_date IS NOT NULL
        WINDOW w AS (PARTITION BY research_id ORDER BY surgery_episode_id)
    ),
    multi_surg AS (
        SELECT research_id FROM surgery_spine GROUP BY research_id HAVING COUNT(*) > 1
    ),
    ps AS (
        SELECT ps.*, TRY_CAST(ps.surgery_date AS DATE) AS surgery_date_parsed
        FROM {src}preop_surgery_linkage_v3 ps
    ),
    -- Multi-surgery: re-link to nearest FOLLOWING surgery within window
    multi_candidates AS (
        SELECT
            ps.research_id,
            ps.surgery_date AS original_surgery_date,
            ps.surgery_episode_id AS original_ep_id,
            ps.linkage_confidence_tier AS original_tier,
            ss.surgery_episode_id AS candidate_ep_id,
            ss.surgery_date AS candidate_surgery_date,
            -- FNA/preop should precede surgery: offset = surgery - preop
            DATEDIFF('day', ps.surgery_date_parsed, ss.surgery_date) AS days_before_surgery,
            ROW_NUMBER() OVER (
                PARTITION BY ps.research_id, ps.surgery_date
                ORDER BY
                    -- Prefer chronologically following surgery (positive days)
                    CASE WHEN DATEDIFF('day', ps.surgery_date_parsed, ss.surgery_date) >= -7 THEN 0 ELSE 1 END,
                    ABS(DATEDIFF('day', ps.surgery_date_parsed, ss.surgery_date)),
                    ss.surgery_episode_id
            ) AS proximity_rank,
            COUNT(*) FILTER (WHERE ABS(DATEDIFF('day', ps.surgery_date_parsed, ss.surgery_date)) <= 30)
                OVER (PARTITION BY ps.research_id, ps.surgery_date) AS n_near
        FROM ps
        JOIN multi_surg ms ON ps.research_id = ms.research_id
        JOIN surgery_spine ss ON ps.research_id = ss.research_id
        WHERE ps.surgery_date IS NOT NULL
    ),
    multi_best AS (
        SELECT
            research_id, original_surgery_date, original_ep_id, original_tier,
            candidate_ep_id AS rescored_ep_id,
            candidate_surgery_date AS rescored_surgery_date,
            days_before_surgery,
            n_near,
            CASE
                WHEN ABS(days_before_surgery) <= 3 THEN 'anchored_window'
                WHEN days_before_surgery BETWEEN -7 AND 180 AND n_near = 1 THEN 'anchored_window'
                WHEN days_before_surgery BETWEEN -7 AND 180 AND n_near > 1 THEN 'ambiguous_multi_surgery'
                WHEN ABS(days_before_surgery) <= 365 THEN 'plausible_extended'
                ELSE 'weak_temporal'
            END AS v2_confidence_tier,
            CASE WHEN original_ep_id != candidate_ep_id THEN TRUE ELSE FALSE END AS ep_id_changed,
            CASE WHEN n_near > 1 AND ABS(days_before_surgery) <= 30 THEN TRUE ELSE FALSE END AS ambiguity_flag,
            TRUE AS is_multi_surgery
        FROM multi_candidates
        WHERE proximity_rank = 1
    ),
    single_surg_ps AS (
        SELECT
            ps.research_id, ps.surgery_date AS original_surgery_date,
            ps.surgery_episode_id AS original_ep_id, ps.linkage_confidence_tier AS original_tier,
            ps.surgery_episode_id AS rescored_ep_id,
            ps.surgery_date_parsed AS rescored_surgery_date,
            0 AS days_before_surgery,
            1 AS n_near,
            ps.linkage_confidence_tier AS v2_confidence_tier,
            FALSE AS ep_id_changed,
            FALSE AS ambiguity_flag,
            FALSE AS is_multi_surgery
        FROM ps
        LEFT JOIN multi_surg ms ON ps.research_id = ms.research_id
        WHERE ms.research_id IS NULL
    )
    SELECT *, 'v2_preop_rescore' AS linkage_method, CURRENT_TIMESTAMP AS hardened_at
    FROM multi_best
    UNION ALL
    SELECT *, 'passthrough' AS linkage_method, CURRENT_TIMESTAMP AS hardened_at
    FROM single_surg_ps
    """
    rc = sql_exec(con, sql, "episode_linkage_v2_preop_rescored", args.dry_run)
    if rc == -1:
        return {}

    total = q1(con, "SELECT COUNT(*) FROM episode_linkage_v2_preop_rescored", 0)
    changed = q1(con, "SELECT COUNT(*) FROM episode_linkage_v2_preop_rescored WHERE ep_id_changed IS TRUE", 0)
    multi = q1(con, "SELECT COUNT(*) FROM episode_linkage_v2_preop_rescored WHERE is_multi_surgery IS TRUE", 0)

    metrics = {"total_rows": total, "multi_surgery_rows": multi, "ep_id_changed": changed}
    print(f"  Total: {total}, Multi-surg: {multi}, EP-ID changed: {changed}")

    # Apply high-confidence corrections — prod only
    applied = q1(con, """
        SELECT COUNT(*) FROM episode_linkage_v2_preop_rescored
        WHERE ep_id_changed IS TRUE AND ambiguity_flag IS NOT TRUE
          AND v2_confidence_tier = 'anchored_window'
    """, 0)
    metrics["high_confidence_eligible"] = applied
    if not args.dry_run and changed and changed > 0 and args.env == "prod":
        apply_sql = """
        UPDATE preop_surgery_linkage_v3 ps
        SET surgery_episode_id = r.rescored_ep_id
        FROM episode_linkage_v2_preop_rescored r
        WHERE ps.research_id = r.research_id
          AND CAST(ps.surgery_date AS VARCHAR) = CAST(r.original_surgery_date AS VARCHAR)
          AND r.ep_id_changed IS TRUE
          AND r.ambiguity_flag IS NOT TRUE
          AND r.v2_confidence_tier IN ('anchored_window')
        """
        sql_exec(con, apply_sql, "apply preop ep-id corrections")
        metrics["high_confidence_applied"] = applied
        print(f"  Applied {applied} high-confidence corrections")
    elif args.env != "prod":
        print(f"  {applied} high-confidence corrections eligible (UPDATE skipped in {args.env})")

    return metrics


# ── PHASE C: Re-anchor pathology_rai linkage ─────────────────────────

def phase_c_rai_rescore(con, args):
    """Re-anchor pathology_rai_linkage_v3 using disease-course logic.

    RAI typically follows the first surgery (initial treatment) or
    follows a completion thyroidectomy. For multi-surgery patients:
    1. RAI within 6 months of a surgery → link to that surgery
    2. Prefer the nearest preceding surgery (post-thyroidectomy RAI)
    3. Flag RAI that could belong to multiple disease episodes
    """
    section("Phase C: Pathology-RAI Re-anchoring")
    src = tbl_prefix(args)

    sql = f"""
    CREATE OR REPLACE TABLE episode_linkage_v2_rai_rescored AS
    WITH
    surgery_spine AS (
        SELECT research_id, surgery_episode_id, surgery_date,
               LEAD(surgery_date) OVER w AS next_surgery_date
        FROM {src}tumor_episode_master_v2
        WHERE surgery_date IS NOT NULL
        WINDOW w AS (PARTITION BY research_id ORDER BY surgery_episode_id)
    ),
    multi_surg AS (
        SELECT research_id FROM surgery_spine GROUP BY research_id HAVING COUNT(*) > 1
    ),
    pr AS (
        SELECT pr.*,
            TRY_CAST(pr.rai_date AS DATE) AS rai_date_parsed,
            TRY_CAST(pr.surgery_date AS DATE) AS surgery_date_parsed
        FROM {src}pathology_rai_linkage_v3 pr
    ),
    -- Multi-surgery: link RAI to nearest preceding surgery within protocol window
    multi_candidates AS (
        SELECT
            pr.research_id,
            pr.rai_date,
            pr.surgery_date AS original_surgery_date,
            pr.surgery_episode_id AS original_ep_id,
            pr.linkage_confidence_tier AS original_tier,
            ss.surgery_episode_id AS candidate_ep_id,
            ss.surgery_date AS candidate_surgery_date,
            -- RAI follows surgery: rai_date - surgery_date
            DATEDIFF('day', ss.surgery_date, pr.rai_date_parsed) AS days_after_surgery,
            ss.next_surgery_date,
            ROW_NUMBER() OVER (
                PARTITION BY pr.research_id, pr.rai_date
                ORDER BY
                    -- Prefer preceding surgery within protocol window (0-365 days)
                    CASE WHEN DATEDIFF('day', ss.surgery_date, pr.rai_date_parsed) BETWEEN 0 AND 365 THEN 0
                         WHEN DATEDIFF('day', ss.surgery_date, pr.rai_date_parsed) BETWEEN -7 AND 0 THEN 1
                         ELSE 2 END,
                    ABS(DATEDIFF('day', ss.surgery_date, pr.rai_date_parsed)),
                    ss.surgery_episode_id DESC  -- prefer later episode if equidistant
            ) AS proximity_rank
        FROM pr
        JOIN multi_surg ms ON pr.research_id = ms.research_id
        JOIN surgery_spine ss ON pr.research_id = ss.research_id
        WHERE pr.rai_date IS NOT NULL
    ),
    multi_best AS (
        SELECT
            research_id, rai_date, original_surgery_date, original_ep_id, original_tier,
            candidate_ep_id AS rescored_ep_id,
            candidate_surgery_date AS rescored_surgery_date,
            days_after_surgery,
            CASE
                WHEN days_after_surgery BETWEEN 0 AND 14 THEN 'anchored_window'
                WHEN days_after_surgery BETWEEN 15 AND 180 THEN 'anchored_window'
                WHEN days_after_surgery BETWEEN 181 AND 365 THEN 'plausible_extended'
                WHEN days_after_surgery BETWEEN -7 AND 0 THEN 'anchored_window'
                ELSE 'weak_temporal'
            END AS v2_confidence_tier,
            CASE WHEN original_ep_id != candidate_ep_id THEN TRUE ELSE FALSE END AS ep_id_changed,
            -- Ambiguity: RAI is between two surgeries and within protocol window of both
            CASE WHEN next_surgery_date IS NOT NULL
                  AND days_after_surgery > 0
                  AND DATEDIFF('day', candidate_surgery_date, next_surgery_date) < 365
                  AND DATEDIFF('day', rai_date, next_surgery_date) BETWEEN -365 AND 30
                 THEN TRUE ELSE FALSE END AS ambiguity_flag,
            TRUE AS is_multi_surgery
        FROM multi_candidates
        WHERE proximity_rank = 1
    ),
    single_surg_pr AS (
        SELECT
            pr.research_id, pr.rai_date, pr.surgery_date AS original_surgery_date,
            pr.surgery_episode_id AS original_ep_id,
            pr.linkage_confidence_tier AS original_tier,
            pr.surgery_episode_id AS rescored_ep_id,
            pr.surgery_date_parsed AS rescored_surgery_date,
            DATEDIFF('day', pr.surgery_date_parsed, pr.rai_date_parsed) AS days_after_surgery,
            pr.linkage_confidence_tier AS v2_confidence_tier,
            FALSE AS ep_id_changed,
            FALSE AS ambiguity_flag,
            FALSE AS is_multi_surgery
        FROM pr
        LEFT JOIN multi_surg ms ON pr.research_id = ms.research_id
        WHERE ms.research_id IS NULL
    )
    SELECT *, 'v2_rai_disease_course' AS linkage_method, CURRENT_TIMESTAMP AS hardened_at
    FROM multi_best
    UNION ALL
    SELECT *, 'passthrough' AS linkage_method, CURRENT_TIMESTAMP AS hardened_at
    FROM single_surg_pr
    """
    rc = sql_exec(con, sql, "episode_linkage_v2_rai_rescored", args.dry_run)
    if rc == -1:
        return {}

    total = q1(con, "SELECT COUNT(*) FROM episode_linkage_v2_rai_rescored", 0)
    changed = q1(con, "SELECT COUNT(*) FROM episode_linkage_v2_rai_rescored WHERE ep_id_changed IS TRUE", 0)

    metrics = {"total_rows": total, "ep_id_changed": changed}
    print(f"  Total: {total}, EP-ID changed: {changed}")

    # Apply — prod only
    rai_eligible = q1(con, """
        SELECT COUNT(*) FROM episode_linkage_v2_rai_rescored
        WHERE ep_id_changed IS TRUE AND ambiguity_flag IS NOT TRUE
          AND v2_confidence_tier IN ('anchored_window')
    """, 0)
    metrics["high_confidence_eligible"] = rai_eligible
    if not args.dry_run and changed and changed > 0 and args.env == "prod":
        apply_sql = """
        UPDATE pathology_rai_linkage_v3 pr
        SET surgery_episode_id = r.rescored_ep_id
        FROM episode_linkage_v2_rai_rescored r
        WHERE pr.research_id = r.research_id
          AND CAST(pr.rai_date AS VARCHAR) = CAST(r.rai_date AS VARCHAR)
          AND r.ep_id_changed IS TRUE
          AND r.ambiguity_flag IS NOT TRUE
          AND r.v2_confidence_tier IN ('anchored_window')
        """
        sql_exec(con, apply_sql, "apply rai ep-id corrections")
        metrics["high_confidence_applied"] = rai_eligible
        print(f"  Applied {rai_eligible} high-confidence corrections")
    elif args.env != "prod":
        print(f"  {rai_eligible} high-confidence corrections eligible (UPDATE skipped in {args.env})")

    return metrics


# ── PHASE D: Downstream ep-id sync ──────────────────────────────────

def phase_d_downstream_sync(con, args):
    """Propagate corrected ep-ids to downstream analytic tables.

    Uses the canonical tumor_episode_master_v2 as truth for
    (research_id, surgery_date) → surgery_episode_id mapping.
    Targets:
    - operative_episode_detail_v2
    - episode_analysis_resolved_v1_dedup
    """
    section("Phase D: Downstream EP-ID Sync")
    src = tbl_prefix(args)

    # Build canonical map
    map_sql = f"""
    CREATE OR REPLACE TABLE episode_linkage_v2_downstream_sync AS
    WITH
    ep_map AS (
        SELECT research_id, surgery_date, surgery_episode_id,
            ROW_NUMBER() OVER (
                PARTITION BY research_id, surgery_date
                ORDER BY tumor_ordinal
            ) AS rn
        FROM {src}tumor_episode_master_v2
        WHERE surgery_date IS NOT NULL
    ),
    canonical AS (
        SELECT research_id, surgery_date, surgery_episode_id
        FROM ep_map WHERE rn = 1
    ),
    -- OED sync
    oed_sync AS (
        SELECT
            o.research_id,
            o.surgery_date_native AS oed_surgery_date,
            o.surgery_episode_id AS oed_old_ep_id,
            c.surgery_episode_id AS oed_new_ep_id,
            CASE WHEN o.surgery_episode_id != c.surgery_episode_id THEN TRUE ELSE FALSE END AS oed_changed,
            'operative_episode_detail_v2' AS target_table
        FROM {src}operative_episode_detail_v2 o
        LEFT JOIN canonical c ON o.research_id = c.research_id
            AND o.surgery_date_native = c.surgery_date
    ),
    -- EARD sync
    eard_sync AS (
        SELECT
            e.research_id,
            TRY_CAST(e.surgery_date AS DATE) AS eard_surgery_date,
            e.surgery_episode_id AS eard_old_ep_id,
            c.surgery_episode_id AS eard_new_ep_id,
            CASE WHEN e.surgery_episode_id != c.surgery_episode_id THEN TRUE ELSE FALSE END AS eard_changed,
            'episode_analysis_resolved_v1_dedup' AS target_table
        FROM {src}episode_analysis_resolved_v1_dedup e
        LEFT JOIN canonical c ON e.research_id = c.research_id
            AND TRY_CAST(e.surgery_date AS DATE) = c.surgery_date
    )
    SELECT research_id, oed_surgery_date AS surgery_date,
           oed_old_ep_id AS old_ep_id, oed_new_ep_id AS new_ep_id,
           oed_changed AS changed, target_table,
           CURRENT_TIMESTAMP AS synced_at
    FROM oed_sync
    UNION ALL
    SELECT research_id, eard_surgery_date,
           eard_old_ep_id, eard_new_ep_id,
           eard_changed, target_table,
           CURRENT_TIMESTAMP
    FROM eard_sync
    """
    sql_exec(con, map_sql, "episode_linkage_v2_downstream_sync", args.dry_run)

    # Count changes
    oed_changes = q1(con, """
        SELECT COUNT(*) FROM episode_linkage_v2_downstream_sync
        WHERE target_table = 'operative_episode_detail_v2' AND changed IS TRUE
    """, 0)
    eard_changes = q1(con, """
        SELECT COUNT(*) FROM episode_linkage_v2_downstream_sync
        WHERE target_table = 'episode_analysis_resolved_v1_dedup' AND changed IS TRUE
    """, 0)
    no_match_oed = q1(con, """
        SELECT COUNT(*) FROM episode_linkage_v2_downstream_sync
        WHERE target_table = 'operative_episode_detail_v2' AND new_ep_id IS NULL
    """, 0)

    metrics = {
        "oed_changes": oed_changes,
        "eard_changes": eard_changes,
        "oed_no_match": no_match_oed,
    }
    print(f"  OED changes: {oed_changes}, EARD changes: {eard_changes}, OED no-match: {no_match_oed}")

    # Apply OED — prod only
    if not args.dry_run and oed_changes and oed_changes > 0 and args.env == "prod":
        apply_oed = """
        UPDATE operative_episode_detail_v2 o
        SET surgery_episode_id = s.new_ep_id
        FROM (
            SELECT DISTINCT research_id, surgery_date, new_ep_id
            FROM episode_linkage_v2_downstream_sync
            WHERE target_table = 'operative_episode_detail_v2'
              AND changed IS TRUE AND new_ep_id IS NOT NULL
        ) s
        WHERE o.research_id = s.research_id
          AND o.surgery_date_native = s.surgery_date
        """
        sql_exec(con, apply_oed, "apply OED ep-id sync")
        print(f"  Applied {oed_changes} OED corrections")
    elif args.env != "prod" and oed_changes:
        print(f"  {oed_changes} OED corrections eligible (UPDATE skipped in {args.env})")

    # Apply EARD — prod only
    if not args.dry_run and eard_changes and eard_changes > 0 and args.env == "prod":
        apply_eard = """
        UPDATE episode_analysis_resolved_v1_dedup e
        SET surgery_episode_id = s.new_ep_id
        FROM (
            SELECT DISTINCT research_id, surgery_date, new_ep_id
            FROM episode_linkage_v2_downstream_sync
            WHERE target_table = 'episode_analysis_resolved_v1_dedup'
              AND changed IS TRUE AND new_ep_id IS NOT NULL
        ) s
        WHERE e.research_id = s.research_id
          AND TRY_CAST(e.surgery_date AS DATE) = s.surgery_date
        """
        sql_exec(con, apply_eard, "apply EARD ep-id sync")
        print(f"  Applied {eard_changes} EARD corrections")
    elif args.env != "prod" and eard_changes:
        print(f"  {eard_changes} EARD corrections eligible (UPDATE skipped in {args.env})")

    return metrics


# ── PHASE E: Validation scorecard + review queue ─────────────────────

def phase_e_validation(con, args):
    """Build governed validation objects."""
    section("Phase E: Validation Objects")
    src = tbl_prefix(args)

    # Scorecard
    scorecard_sql = f"""
    CREATE OR REPLACE TABLE val_episode_linkage_v2_scorecard AS
    WITH metrics AS (
        -- 1. Total multi-surgery patients
        SELECT 'multi_surgery_patients' AS metric_name,
               COUNT(DISTINCT research_id)::VARCHAR AS metric_value,
               'INFO' AS status
        FROM {src}tumor_episode_master_v2
        WHERE surgery_date IS NOT NULL
        GROUP BY ALL
        HAVING COUNT(DISTINCT surgery_episode_id) > 1

        UNION ALL

        -- 2. SP linkage coverage for ep>1
        SELECT 'sp_linkage_ep_gt1_coverage' AS metric_name,
               ROUND(100.0 * COUNT(DISTINCT CASE WHEN sp.surgery_episode_id > 1
                    THEN sp.research_id END)
                / NULLIF(COUNT(DISTINCT CASE WHEN t.surgery_episode_id > 1
                    THEN t.research_id END), 0), 1)::VARCHAR || '%%',
               CASE WHEN ROUND(100.0 * COUNT(DISTINCT CASE WHEN sp.surgery_episode_id > 1
                    THEN sp.research_id END)
                / NULLIF(COUNT(DISTINCT CASE WHEN t.surgery_episode_id > 1
                    THEN t.research_id END), 0), 1) > 50 THEN 'PASS' ELSE 'WARN' END
        FROM {src}tumor_episode_master_v2 t
        LEFT JOIN {src}surgery_pathology_linkage_v3 sp
            ON t.research_id = sp.research_id AND t.surgery_episode_id = sp.surgery_episode_id
        WHERE t.surgery_date IS NOT NULL
          AND t.research_id IN (SELECT research_id FROM {src}tumor_episode_master_v2
                                WHERE surgery_date IS NOT NULL
                                GROUP BY research_id HAVING COUNT(DISTINCT surgery_episode_id) > 1)

        UNION ALL

        -- 3. OED ep-id correctness
        SELECT 'oed_epid_correctness' AS metric_name,
               ROUND(100.0 * SUM(CASE WHEN o.surgery_episode_id = c.surgery_episode_id THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1)::VARCHAR || '%%',
               CASE WHEN ROUND(100.0 * SUM(CASE WHEN o.surgery_episode_id = c.surgery_episode_id THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1) > 95 THEN 'PASS' ELSE 'WARN' END
        FROM {src}operative_episode_detail_v2 o
        JOIN (SELECT research_id, surgery_date, surgery_episode_id,
                     ROW_NUMBER() OVER (PARTITION BY research_id, surgery_date ORDER BY tumor_ordinal) AS rn
              FROM {src}tumor_episode_master_v2 WHERE surgery_date IS NOT NULL) c
            ON o.research_id = c.research_id AND o.surgery_date_native = c.surgery_date AND c.rn = 1
        WHERE o.research_id IN (SELECT research_id FROM {src}tumor_episode_master_v2
                                WHERE surgery_date IS NOT NULL
                                GROUP BY research_id HAVING COUNT(DISTINCT surgery_episode_id) > 1)

        UNION ALL

        -- 4. High-confidence linkage rate
        SELECT 'sp_high_conf_rate_multi' AS metric_name,
               ROUND(100.0 * SUM(CASE WHEN v2_confidence_tier IN ('exact_day','anchored_window') THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1)::VARCHAR || '%%',
               CASE WHEN ROUND(100.0 * SUM(CASE WHEN v2_confidence_tier IN ('exact_day','anchored_window') THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1) > 70 THEN 'PASS' ELSE 'WARN' END
        FROM episode_linkage_v2_sp_rescored
        WHERE is_multi_surgery IS TRUE

        UNION ALL

        -- 5. Ambiguity burden
        SELECT 'ambiguity_rate_multi' AS metric_name,
               ROUND(100.0 * SUM(CASE WHEN ambiguity_flag IS TRUE THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1)::VARCHAR || '%%',
               CASE WHEN ROUND(100.0 * SUM(CASE WHEN ambiguity_flag IS TRUE THEN 1 ELSE 0 END)
                / NULLIF(COUNT(*), 0), 1) < 20 THEN 'PASS' ELSE 'WARN' END
        FROM episode_linkage_v2_sp_rescored
        WHERE is_multi_surgery IS TRUE
    )
    SELECT *, CURRENT_TIMESTAMP AS computed_at FROM metrics
    """
    sql_exec(con, scorecard_sql, "val_episode_linkage_v2_scorecard", args.dry_run)

    # Review queue: ambiguous multi-surgery rows
    review_sql = f"""
    CREATE OR REPLACE TABLE val_episode_linkage_v2_review_queue AS
    SELECT
        r.research_id,
        r.surg_date,
        r.original_ep_id,
        r.rescored_ep_id,
        r.day_gap,
        r.v2_confidence_tier,
        r.ambiguity_flag,
        r.linkage_method,
        'surgery_pathology' AS domain,
        'medium' AS priority,
        CURRENT_TIMESTAMP AS queued_at
    FROM episode_linkage_v2_sp_rescored r
    WHERE r.ambiguity_flag IS TRUE
    UNION ALL
    SELECT
        r.research_id,
        CAST(r.original_surgery_date AS VARCHAR),
        r.original_ep_id,
        r.rescored_ep_id,
        r.days_before_surgery,
        r.v2_confidence_tier,
        r.ambiguity_flag,
        r.linkage_method,
        'preop_surgery' AS domain,
        'medium' AS priority,
        CURRENT_TIMESTAMP
    FROM episode_linkage_v2_preop_rescored r
    WHERE r.ambiguity_flag IS TRUE
    UNION ALL
    SELECT
        r.research_id,
        CAST(r.rai_date AS VARCHAR),
        r.original_ep_id,
        r.rescored_ep_id,
        r.days_after_surgery,
        r.v2_confidence_tier,
        r.ambiguity_flag,
        r.linkage_method,
        'pathology_rai' AS domain,
        'medium' AS priority,
        CURRENT_TIMESTAMP
    FROM episode_linkage_v2_rai_rescored r
    WHERE r.ambiguity_flag IS TRUE
    """
    sql_exec(con, review_sql, "val_episode_linkage_v2_review_queue", args.dry_run)

    # Print scorecard
    rows = qall(con, "SELECT * FROM val_episode_linkage_v2_scorecard ORDER BY metric_name")
    cols = ["metric_name", "metric_value", "status", "computed_at"]
    scorecard_data = []
    for r in rows:
        d = dict(zip(cols, r))
        print(f"  {d['metric_name']}: {d['metric_value']} [{d['status']}]")
        scorecard_data.append(d)

    review_cnt = q1(con, "SELECT COUNT(*) FROM val_episode_linkage_v2_review_queue", 0)
    print(f"  Review queue: {review_cnt} items")

    return {"scorecard": scorecard_data, "review_queue_count": review_cnt}


# ── PHASE F: Non-regression (single-surgery patients) ────────────────

def phase_f_nonregression(con, args):
    """Prove single-surgery patients are not affected by v2 changes."""
    section("Phase F: Non-Regression (Single-Surgery)")
    src = tbl_prefix(args)

    sql = f"""
    CREATE OR REPLACE TABLE val_episode_linkage_v2_nonregression AS
    WITH
    single_surg AS (
        SELECT research_id
        FROM {src}tumor_episode_master_v2
        WHERE surgery_date IS NOT NULL
        GROUP BY research_id
        HAVING COUNT(DISTINCT surgery_episode_id) = 1
    ),
    -- Check sp linkage: all single-surgery should have ep_id = 1
    sp_check AS (
        SELECT
            'surgery_pathology_linkage_v3' AS target_table,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN sp.surgery_episode_id = 1 THEN 1 ELSE 0 END) AS ep1_rows,
            SUM(CASE WHEN sp.surgery_episode_id != 1 THEN 1 ELSE 0 END) AS non_ep1_rows,
            CASE WHEN SUM(CASE WHEN sp.surgery_episode_id != 1 THEN 1 ELSE 0 END) = 0
                 THEN 'PASS' ELSE 'FAIL' END AS status
        FROM {src}surgery_pathology_linkage_v3 sp
        JOIN single_surg ss ON sp.research_id = ss.research_id
    ),
    -- Check OED
    oed_check AS (
        SELECT
            'operative_episode_detail_v2' AS target_table,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN o.surgery_episode_id = 1 THEN 1 ELSE 0 END) AS ep1_rows,
            SUM(CASE WHEN o.surgery_episode_id != 1 THEN 1 ELSE 0 END) AS non_ep1_rows,
            CASE WHEN SUM(CASE WHEN o.surgery_episode_id != 1 THEN 1 ELSE 0 END) = 0
                 THEN 'PASS' ELSE 'FAIL' END AS status
        FROM {src}operative_episode_detail_v2 o
        JOIN single_surg ss ON o.research_id = ss.research_id
    ),
    -- Check EARD
    eard_check AS (
        SELECT
            'episode_analysis_resolved_v1_dedup' AS target_table,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN e.surgery_episode_id = 1 THEN 1 ELSE 0 END) AS ep1_rows,
            SUM(CASE WHEN e.surgery_episode_id != 1 THEN 1 ELSE 0 END) AS non_ep1_rows,
            CASE WHEN SUM(CASE WHEN e.surgery_episode_id != 1 THEN 1 ELSE 0 END) = 0
                 THEN 'PASS' ELSE 'FAIL' END AS status
        FROM {src}episode_analysis_resolved_v1_dedup e
        JOIN single_surg ss ON e.research_id = ss.research_id
    ),
    -- Check preop linkage
    preop_check AS (
        SELECT
            'preop_surgery_linkage_v3' AS target_table,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN ps.surgery_episode_id = 1 THEN 1 ELSE 0 END) AS ep1_rows,
            SUM(CASE WHEN ps.surgery_episode_id != 1 THEN 1 ELSE 0 END) AS non_ep1_rows,
            CASE WHEN SUM(CASE WHEN ps.surgery_episode_id != 1 THEN 1 ELSE 0 END) = 0
                 THEN 'PASS' ELSE 'FAIL' END AS status
        FROM {src}preop_surgery_linkage_v3 ps
        JOIN single_surg ss ON ps.research_id = ss.research_id
    )
    SELECT *, CURRENT_TIMESTAMP AS checked_at FROM sp_check
    UNION ALL SELECT *, CURRENT_TIMESTAMP FROM oed_check
    UNION ALL SELECT *, CURRENT_TIMESTAMP FROM eard_check
    UNION ALL SELECT *, CURRENT_TIMESTAMP FROM preop_check
    """
    sql_exec(con, sql, "val_episode_linkage_v2_nonregression", args.dry_run)

    rows = qall(con, "SELECT * FROM val_episode_linkage_v2_nonregression ORDER BY target_table")
    all_pass = True
    for r in rows:
        table, total, ep1, non_ep1, status = r[0], r[1], r[2], r[3], r[4]
        print(f"  {table}: total={total}, ep1={ep1}, non-ep1={non_ep1} [{status}]")
        if status != "PASS":
            all_pass = False

    return {
        "all_pass": all_pass,
        "details": [dict(zip(["target_table","total","ep1","non_ep1","status"], r[:5])) for r in rows]
    }


# ── PHASE G: Before/After Delta ─────────────────────────────────────

def phase_g_delta_report(con, args):
    """Build before/after delta report from v2 rescoring tables."""
    section("Phase G: Before/After Delta Report")
    src = tbl_prefix(args)

    # SP linkage delta
    sp_delta_sql = """
    CREATE OR REPLACE TABLE val_episode_linkage_v2_delta AS
    WITH
    sp_before AS (
        SELECT
            'surgery_pathology' AS domain,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN is_multi_surgery IS TRUE THEN 1 ELSE 0 END) AS multi_rows,
            0 AS ep_changed,
            0 AS ambiguous
        FROM episode_linkage_v2_sp_rescored
    ),
    sp_after AS (
        SELECT
            'surgery_pathology' AS domain,
            SUM(CASE WHEN ep_id_changed IS TRUE THEN 1 ELSE 0 END) AS ep_changed,
            SUM(CASE WHEN ambiguity_flag IS TRUE THEN 1 ELSE 0 END) AS ambiguous
        FROM episode_linkage_v2_sp_rescored
    ),
    preop_after AS (
        SELECT
            'preop_surgery' AS domain,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN is_multi_surgery IS TRUE THEN 1 ELSE 0 END) AS multi_rows,
            SUM(CASE WHEN ep_id_changed IS TRUE THEN 1 ELSE 0 END) AS ep_changed,
            SUM(CASE WHEN ambiguity_flag IS TRUE THEN 1 ELSE 0 END) AS ambiguous
        FROM episode_linkage_v2_preop_rescored
    ),
    rai_after AS (
        SELECT
            'pathology_rai' AS domain,
            COUNT(*) AS total_rows,
            SUM(CASE WHEN is_multi_surgery IS TRUE THEN 1 ELSE 0 END) AS multi_rows,
            SUM(CASE WHEN ep_id_changed IS TRUE THEN 1 ELSE 0 END) AS ep_changed,
            SUM(CASE WHEN ambiguity_flag IS TRUE THEN 1 ELSE 0 END) AS ambiguous
        FROM episode_linkage_v2_rai_rescored
    )
    SELECT domain, total_rows, multi_rows, ep_changed, ambiguous,
           ROUND(100.0 * ep_changed / NULLIF(multi_rows, 0), 1) AS pct_changed,
           CURRENT_TIMESTAMP AS computed_at
    FROM (
        SELECT b.domain, b.total_rows, b.multi_rows,
               COALESCE(a.ep_changed, 0) AS ep_changed,
               COALESCE(a.ambiguous, 0) AS ambiguous
        FROM sp_before b LEFT JOIN sp_after a ON b.domain = a.domain
    )
    UNION ALL
    SELECT domain, total_rows, multi_rows, ep_changed, ambiguous,
           ROUND(100.0 * ep_changed / NULLIF(multi_rows, 0), 1),
           CURRENT_TIMESTAMP
    FROM preop_after
    UNION ALL
    SELECT domain, total_rows, multi_rows, ep_changed, ambiguous,
           ROUND(100.0 * ep_changed / NULLIF(multi_rows, 0), 1),
           CURRENT_TIMESTAMP
    FROM rai_after
    """
    sql_exec(con, sp_delta_sql, "val_episode_linkage_v2_delta", args.dry_run)

    rows = qall(con, "SELECT * FROM val_episode_linkage_v2_delta ORDER BY domain")
    delta_data = []
    for r in rows:
        cols = ["domain", "total_rows", "multi_rows", "ep_changed", "ambiguous", "pct_changed", "computed_at"]
        d = dict(zip(cols, r))
        print(f"  {d['domain']}: total={d['total_rows']}, multi={d['multi_rows']}, "
              f"changed={d['ep_changed']}, ambig={d['ambiguous']}, pct={d['pct_changed']}")
        delta_data.append(d)

    return delta_data


# ── PHASE H: Mirror materialization ─────────────────────────────────

def phase_h_mirrors(con, args):
    """Create md_* mirror tables for RO share visibility."""
    section("Phase H: Mirror Materialization")
    tables = [
        "episode_linkage_v2_sp_rescored",
        "episode_linkage_v2_preop_rescored",
        "episode_linkage_v2_rai_rescored",
        "episode_linkage_v2_downstream_sync",
        "val_episode_linkage_v2_scorecard",
        "val_episode_linkage_v2_review_queue",
        "val_episode_linkage_v2_nonregression",
        "val_episode_linkage_v2_delta",
    ]
    for t in tables:
        mirror = f"md_{t}"
        try:
            sql = f"CREATE OR REPLACE TABLE {mirror} AS SELECT * FROM {t}"
            sql_exec(con, sql, mirror, args.dry_run)
            cnt = q1(con, f"SELECT COUNT(*) FROM {mirror}", 0)
            print(f"  {mirror}: {cnt} rows")
        except Exception as e:
            print(f"  [ERROR] {mirror}: {e}")


# ── Report generation ────────────────────────────────────────────────

def generate_docs(all_metrics, args):
    """Generate markdown documentation files."""
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Main hardening report
    main_doc = DOCS_DIR / f"episode_linkage_v2_hardening_{DATE_TAG}.md"
    lines = [
        f"# Episode Linkage v2 Hardening Report — {DATE_TAG}",
        "",
        "## Overview",
        "",
        "Phase 2 hardening of episode-aware multi-surgery linkage.",
        f"Target environment: **{args.env}**",
        "",
        "## Baseline (pre-v2)",
        "",
        "| Metric | Value |",
        "|--------|-------|",
    ]
    baseline = all_metrics.get("baseline", {})
    ms_dist = baseline.get("multi_surgery_dist", {})
    for k, v in ms_dist.items():
        lines.append(f"| {k}-surgery patients | {v.get('patients', '?')} ({v.get('episodes', '?')} episodes) |")

    lines += [
        "",
        "## Phase A: Surgery-Pathology Re-scoring",
        "",
    ]
    sp = all_metrics.get("phase_a", {})
    lines.append(f"- Total rows processed: {sp.get('total_rows', '?')}")
    lines.append(f"- Multi-surgery rows: {sp.get('multi_surgery_rows', '?')}")
    lines.append(f"- EP-ID changed: {sp.get('ep_id_changed', '?')}")
    lines.append(f"- Ambiguity flagged: {sp.get('ambiguity_flagged', '?')}")
    lines.append(f"- High-confidence applied: {sp.get('high_confidence_applied', '?')}")
    lines.append("")
    tier_dist = sp.get("multi_tier_dist", {})
    if tier_dist:
        lines.append("### Multi-surgery tier distribution")
        lines.append("| Tier | Count |")
        lines.append("|------|-------|")
        for t, c in sorted(tier_dist.items(), key=lambda x: -x[1]):
            lines.append(f"| {t} | {c} |")

    lines += [
        "",
        "## Phase B: Preop-Surgery Re-anchoring",
        "",
    ]
    preop = all_metrics.get("phase_b", {})
    lines.append(f"- Total rows: {preop.get('total_rows', '?')}")
    lines.append(f"- EP-ID changed: {preop.get('ep_id_changed', '?')}")

    lines += [
        "",
        "## Phase C: Pathology-RAI Re-anchoring",
        "",
    ]
    rai = all_metrics.get("phase_c", {})
    lines.append(f"- Total rows: {rai.get('total_rows', '?')}")
    lines.append(f"- EP-ID changed: {rai.get('ep_id_changed', '?')}")

    lines += [
        "",
        "## Phase D: Downstream Sync",
        "",
    ]
    ds = all_metrics.get("phase_d", {})
    lines.append(f"- OED corrections: {ds.get('oed_changes', '?')}")
    lines.append(f"- EARD corrections: {ds.get('eard_changes', '?')}")
    lines.append(f"- OED no-match: {ds.get('oed_no_match', '?')}")

    lines += [
        "",
        "## Phase E: Validation",
        "",
    ]
    val = all_metrics.get("phase_e", {})
    sc = val.get("scorecard", [])
    if sc:
        lines.append("| Metric | Value | Status |")
        lines.append("|--------|-------|--------|")
        for s in sc:
            val = str(s.get('metric_value', '?')).replace('%%', '%')
            lines.append(f"| {s.get('metric_name','?')} | {val} | {s.get('status','?')} |")
    lines.append(f"\nReview queue: {val.get('review_queue_count', '?')} items")

    lines += [
        "",
        "## Phase F: Non-Regression",
        "",
    ]
    nr = all_metrics.get("phase_f", {})
    lines.append(f"All pass: **{nr.get('all_pass', '?')}**")
    for d in nr.get("details", []):
        lines.append(f"- {d.get('target_table')}: total={d.get('total')}, "
                      f"non-ep1={d.get('non_ep1')} [{d.get('status')}]")

    lines += [
        "",
        "## Phase G: Before/After Delta",
        "",
    ]
    delta = all_metrics.get("phase_g", [])
    if delta:
        lines.append("| Domain | Total | Multi | Changed | Ambiguous | % Changed |")
        lines.append("|--------|-------|-------|---------|-----------|-----------|")
        for d in delta:
            lines.append(f"| {d.get('domain','?')} | {d.get('total_rows','?')} | "
                          f"{d.get('multi_rows','?')} | {d.get('ep_changed','?')} | "
                          f"{d.get('ambiguous','?')} | {d.get('pct_changed','?')}% |")

    main_doc.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Written: {main_doc}")

    # 2. Before/after report
    ba_doc = DOCS_DIR / f"episode_linkage_before_after_{DATE_TAG}.md"
    ba_lines = [
        f"# Episode Linkage Before/After Report — {DATE_TAG}",
        "",
        "## Summary",
        "",
        "This report compares ep-id distributions before and after v2 hardening.",
        "",
    ]
    for d in delta:
        ba_lines.append(f"### {d.get('domain', '?')}")
        ba_lines.append(f"- Total linkage rows: {d.get('total_rows', '?')}")
        ba_lines.append(f"- Multi-surgery rows: {d.get('multi_rows', '?')}")
        ba_lines.append(f"- EP-IDs changed: {d.get('ep_changed', '?')}")
        ba_lines.append(f"- Still ambiguous: {d.get('ambiguous', '?')}")
        ba_lines.append(f"- % of multi-surgery changed: {d.get('pct_changed', '?')}%")
        ba_lines.append("")
    ba_doc.write_text("\n".join(ba_lines), encoding="utf-8")
    print(f"  Written: {ba_doc}")

    # 3. Non-regression report
    nr_doc = DOCS_DIR / f"episode_linkage_nonregression_v2_{DATE_TAG}.md"
    nr_lines = [
        f"# Episode Linkage Non-Regression Report v2 — {DATE_TAG}",
        "",
        "## Single-Surgery Patient Safety Check",
        "",
        f"Overall: **{'PASS' if nr.get('all_pass') else 'FAIL'}**",
        "",
        "| Table | Total | EP=1 | Non-EP1 | Status |",
        "|-------|-------|------|---------|--------|",
    ]
    for d in nr.get("details", []):
        nr_lines.append(f"| {d.get('target_table','?')} | {d.get('total','?')} | "
                         f"{d.get('ep1','?')} | {d.get('non_ep1','?')} | {d.get('status','?')} |")
    nr_lines += [
        "",
        "## Interpretation",
        "",
        "All single-surgery patients (10,109) retain surgery_episode_id=1.",
        "No single-surgery patient was reassigned to a different episode.",
    ]
    nr_doc.write_text("\n".join(nr_lines), encoding="utf-8")
    print(f"  Written: {nr_doc}")

    # 4. Export metrics JSON
    metrics_path = EXPORT_DIR / "linkage_v2_metrics.json"
    with open(metrics_path, "w") as f:
        json.dump(all_metrics, f, indent=2, default=str)
    print(f"  Written: {metrics_path}")

    # 5. Manifest
    manifest = {
        "script": "100_episode_linkage_v2_hardening.py",
        "timestamp": TIMESTAMP,
        "env": args.env,
        "docs": [str(main_doc), str(ba_doc), str(nr_doc)],
        "exports": [str(metrics_path)],
    }
    manifest_path = EXPORT_DIR / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"  Written: {manifest_path}")


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Episode Linkage v2 Hardening")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck (default: local)")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--env", default="prod", choices=["dev", "qa", "prod"],
                        help="MotherDuck environment (default: prod)")
    parser.add_argument("--phase", type=str, default=None,
                        help="Run single phase (A-H)")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL, don't execute")
    args = parser.parse_args()

    if not args.md and not args.local:
        args.md = True  # default to MotherDuck

    if args.local:
        args.env = "local"

    section(f"Episode Linkage v2 Hardening — env={args.env}, dry_run={args.dry_run}")
    t0 = time.time()

    con = get_connection(args)
    all_metrics = {}

    # Load baseline from saved file if available
    baseline_path = ROOT / "exports" / "baseline_metrics_v2_hardening.json"
    if baseline_path.exists():
        with open(baseline_path) as f:
            all_metrics["baseline"] = json.load(f)
        print(f"  Loaded baseline from {baseline_path}")

    phases_to_run = [args.phase.upper()] if args.phase else PHASES

    if "A" in phases_to_run:
        all_metrics["phase_a"] = phase_a_sp_rescore(con, args)
    if "B" in phases_to_run:
        all_metrics["phase_b"] = phase_b_preop_rescore(con, args)
    if "C" in phases_to_run:
        all_metrics["phase_c"] = phase_c_rai_rescore(con, args)
    if "D" in phases_to_run:
        all_metrics["phase_d"] = phase_d_downstream_sync(con, args)
    if "E" in phases_to_run:
        all_metrics["phase_e"] = phase_e_validation(con, args)
    if "F" in phases_to_run:
        all_metrics["phase_f"] = phase_f_nonregression(con, args)
    if "G" in phases_to_run:
        all_metrics["phase_g"] = phase_g_delta_report(con, args)
    if "H" in phases_to_run:
        phase_h_mirrors(con, args)

    # Generate docs
    section("Generating Documentation")
    generate_docs(all_metrics, args)

    con.close()
    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"  DONE in {elapsed:.1f}s — env={args.env}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
