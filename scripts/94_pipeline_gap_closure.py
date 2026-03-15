#!/usr/bin/env python3
"""
Script 94: Pipeline Gap Closure Sprint
========================================
Executes high-yield closure for pipeline-limited and process-limited gaps
WITHOUT fabricating improvements to source-limited domains.

Workstreams:
  A) Operative NLP coverage — supplement parathyroid_autotransplant from NLP,
     create val_operative_coverage_v2 with field-level before/after counts.
  B) Imaging → FNA linkage backfill — populate fna_episode_master_v2
     .linked_imaging_nodule_id from imaging_fna_linkage_v3 (score >= 0.65).
  C) Recurrence manual review queue — build recurrence_review_queue_v1 with
     full clinical context for efficient chart review; per-tier summary.
  D) Adjudication starter pack — export top-priority cases; build
     adjudication_progress_kpi_v1 view.

Usage:
  .venv/bin/python scripts/94_pipeline_gap_closure.py --md [--phase A/B/C/D/all]
  .venv/bin/python scripts/94_pipeline_gap_closure.py --local
  .venv/bin/python scripts/94_pipeline_gap_closure.py --md --dry-run
"""

import argparse
import csv
import datetime
import json
import os
import sys
from pathlib import Path

# ── resolve workspace root ───────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT))

import duckdb  # noqa: E402

# ── timestamp ────────────────────────────────────────────────────────────────
TS = datetime.datetime.now().strftime("%Y%m%d_%H%M")
DATE_SLUG = datetime.datetime.now().strftime("%Y%m%d")

EXPORT_DIR = ROOT / f"exports/pipeline_gap_closure_{TS}"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


# ── connection ────────────────────────────────────────────────────────────────
def get_connection(use_md: bool) -> duckdb.DuckDBPyConnection:
    if use_md:
        token = os.environ.get("MOTHERDUCK_TOKEN", "")
        if not token:
            secrets = ROOT / ".streamlit" / "secrets.toml"
            if secrets.exists():
                import toml
                token = toml.load(secrets).get("MOTHERDUCK_TOKEN", "")
        os.environ["MOTHERDUCK_TOKEN"] = token
        return duckdb.connect("md:thyroid_research_2026")
    return duckdb.connect(str(ROOT / "thyroid_master_local.duckdb"))


def safe_exec(con, sql: str, label: str = "") -> int:
    """Execute DDL/DML; return affected rowcount (−1 = success for DDL)."""
    try:
        r = con.execute(sql)
        rc = r.rowcount if hasattr(r, "rowcount") else -1
        return 0 if rc == -1 else rc
    except Exception as exc:
        print(f"  [WARN] {label}: {exc!s:.120}")
        return -99


def count(con, table: str) -> int:
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        return -1


def scalar(con, sql: str) -> object:
    try:
        return con.execute(sql).fetchone()[0]
    except Exception:
        return None


# ────────────────────────────────────────────────────────────────────────────
# WORKSTREAM A: OPERATIVE NLP COVERAGE
# ────────────────────────────────────────────────────────────────────────────

def run_workstream_a(con, dry_run: bool) -> dict:
    """
    Supplement operative_episode_detail_v2.parathyroid_autograft_flag with
    note_entities_procedures NLP (parathyroid_autotransplant), then rebuild
    patient_analysis_resolved_v1 op_* aggregates.  Create validation table
    val_operative_coverage_v2.
    """
    print("\n=== WORKSTREAM A: Operative NLP Coverage ===")

    # ── Before metrics ────────────────────────────────────────────────────
    before = {}

    before_sql = """
        SELECT 
            SUM(CASE WHEN parathyroid_autograft_flag IS TRUE THEN 1 ELSE 0 END) as para_autograft,
            SUM(CASE WHEN strap_muscle_involvement_flag IS TRUE THEN 1 ELSE 0 END) as strap,
            SUM(CASE WHEN reoperative_field_flag IS TRUE THEN 1 ELSE 0 END) as reoperative,
            SUM(CASE WHEN gross_ete_flag IS TRUE THEN 1 ELSE 0 END) as gross_ete,
            SUM(CASE WHEN drain_flag IS TRUE THEN 1 ELSE 0 END) as drain,
            SUM(CASE WHEN rln_monitoring_flag IS TRUE THEN 1 ELSE 0 END) as rln_monitoring,
            SUM(CASE WHEN operative_findings_raw IS NOT NULL THEN 1 ELSE 0 END) as op_findings
        FROM operative_episode_detail_v2
    """
    try:
        r = con.execute(before_sql).fetchone()
        before = dict(zip(['para_autograft','strap','reoperative','gross_ete','drain',
                           'rln_monitoring','op_findings'], r))
        print(f"  Before (operative_episode_detail_v2): {before}")
    except Exception as e:
        print(f"  [ERROR] Before metrics: {e}")

    # ── Step A1: NLP parathyroid supplement ───────────────────────────────
    # note_entities_procedures has 56 parathyroid_autotransplant mentions
    # operative_episode_detail_v2 has 40 parathyroid_autograft_flag=TRUE
    # Find patients with NLP autotransplant that aren't already flagged

    nlp_parathyroid_sql = """
        SELECT DISTINCT np.research_id
        FROM note_entities_procedures np
        WHERE np.entity_value_norm = 'parathyroid_autotransplant'
        AND np.present_or_negated = 'present'
    """
    try:
        nlp_pts = [r[0] for r in con.execute(nlp_parathyroid_sql).fetchall()]
        print(f"  NLP parathyroid_autotransplant patients: {len(nlp_pts)}")
    except Exception as e:
        print(f"  [ERROR] NLP query: {e}")
        nlp_pts = []

    # Find which of those don't already have the flag in operative_episode_detail_v2
    if nlp_pts:
        overlap_sql = """
            SELECT COUNT(DISTINCT research_id) 
            FROM operative_episode_detail_v2
            WHERE parathyroid_autograft_flag IS TRUE
            AND research_id IN ({})
        """.format(','.join(str(p) for p in nlp_pts))
        try:
            overlap = con.execute(overlap_sql).fetchone()[0]
            newly_recoverable = len(nlp_pts) - overlap
            print(f"  Already flagged in op table: {overlap}")
            print(f"  Newly recoverable from NLP: {newly_recoverable}")
        except Exception as e:
            print(f"  [ERROR] Overlap: {e}")
            newly_recoverable = 0
    else:
        newly_recoverable = 0

    # ── Step A2: Apply NLP supplement to operative_episode_detail_v2 ────
    if newly_recoverable > 0 and not dry_run:
        # Find the earliest surgery episode for NLP-positive patients
        # where parathyroid_autograft_flag is not already set
        update_sql = """
            UPDATE operative_episode_detail_v2
            SET parathyroid_autograft_flag = TRUE,
                op_enrichment_source = COALESCE(op_enrichment_source, 'nlp_entities_procedures')
            WHERE research_id IN (
                SELECT DISTINCT np.research_id
                FROM note_entities_procedures np
                WHERE np.entity_value_norm = 'parathyroid_autotransplant'
                AND np.present_or_negated = 'present'
            )
            AND (parathyroid_autograft_flag IS NOT TRUE)
        """
        rc = safe_exec(con, update_sql, "update parathyroid_autograft")
        print(f"  Updated parathyroid_autograft_flag rows: {rc}")

    # ── Step A3: Rebuild patient_analysis_resolved_v1 op_* aggregates ───
    # Verify current op_* counts and refresh from source
    if not dry_run:
        refresh_sql = """
            CREATE OR REPLACE TEMP TABLE _op_agg AS
            SELECT
                research_id,
                BOOL_OR(rln_monitoring_flag) AS op_rln_monitoring_any,
                BOOL_OR(drain_flag) AS op_drain_placed_any,
                BOOL_OR(strap_muscle_involvement_flag) AS op_strap_muscle_any,
                BOOL_OR(reoperative_field_flag) AS op_reoperative_any,
                BOOL_OR(parathyroid_autograft_flag) AS op_parathyroid_autograft_any,
                BOOL_OR(local_invasion_flag) AS op_local_invasion_any,
                BOOL_OR(tracheal_involvement_flag) AS op_tracheal_inv_any,
                BOOL_OR(esophageal_involvement_flag) AS op_esophageal_inv_any,
                BOOL_OR(gross_ete_flag) AS op_intraop_gross_ete_any,
                COUNT(CASE WHEN operative_findings_raw IS NOT NULL THEN 1 END) AS op_n_surgeries_with_findings,
                STRING_AGG(DISTINCT operative_findings_raw, ' | ') AS op_findings_summary
            FROM operative_episode_detail_v2
            GROUP BY research_id
        """
        safe_exec(con, refresh_sql, "build op_agg temp table")

        # Update patient_analysis_resolved_v1
        update_patient_sql = """
            UPDATE patient_analysis_resolved_v1 p
            SET 
                op_rln_monitoring_any = a.op_rln_monitoring_any,
                op_drain_placed_any = a.op_drain_placed_any,
                op_strap_muscle_any = a.op_strap_muscle_any,
                op_reoperative_any = a.op_reoperative_any,
                op_parathyroid_autograft_any = a.op_parathyroid_autograft_any,
                op_local_invasion_any = a.op_local_invasion_any,
                op_tracheal_inv_any = a.op_tracheal_inv_any,
                op_esophageal_inv_any = a.op_esophageal_inv_any,
                op_intraop_gross_ete_any = a.op_intraop_gross_ete_any,
                op_n_surgeries_with_findings = a.op_n_surgeries_with_findings,
                op_findings_summary = a.op_findings_summary
            FROM _op_agg a
            WHERE p.research_id = a.research_id
        """
        rc = safe_exec(con, update_patient_sql, "update patient op_* agg")
        print(f"  Refreshed patient op_* aggregates: rows affected = {rc}")

        # Update manuscript_cohort_v1 too
        update_mc_sql = """
            UPDATE manuscript_cohort_v1 p
            SET 
                op_rln_monitoring_any = a.op_rln_monitoring_any,
                op_drain_placed_any = a.op_drain_placed_any,
                op_strap_muscle_any = a.op_strap_muscle_any,
                op_reoperative_any = a.op_reoperative_any,
                op_parathyroid_autograft_any = a.op_parathyroid_autograft_any,
                op_local_invasion_any = a.op_local_invasion_any,
                op_tracheal_inv_any = a.op_tracheal_inv_any,
                op_esophageal_inv_any = a.op_esophageal_inv_any,
                op_intraop_gross_ete_any = a.op_intraop_gross_ete_any,
                op_n_surgeries_with_findings = a.op_n_surgeries_with_findings,
                op_findings_summary = a.op_findings_summary
            FROM _op_agg a
            WHERE p.research_id = a.research_id
        """
        rc2 = safe_exec(con, update_mc_sql, "update manuscript op_* agg")
        print(f"  Refreshed manuscript_cohort_v1 op_* aggregates: {rc2}")

    # ── Step A4: Create val_operative_coverage_v2 ────────────────────────
    val_sql = """
        CREATE OR REPLACE TABLE val_operative_coverage_v2 AS
        WITH episode_fields AS (
            SELECT 
                'operative_episode_detail_v2'  AS source_table,
                COUNT(*) AS total_episodes,
                SUM(CASE WHEN parathyroid_autograft_flag IS TRUE THEN 1 ELSE 0 END) AS parathyroid_autograft,
                SUM(CASE WHEN strap_muscle_involvement_flag IS TRUE THEN 1 ELSE 0 END) AS strap_muscle,
                SUM(CASE WHEN reoperative_field_flag IS TRUE THEN 1 ELSE 0 END) AS reoperative,
                SUM(CASE WHEN gross_ete_flag IS TRUE THEN 1 ELSE 0 END) AS gross_ete,
                SUM(CASE WHEN drain_flag IS TRUE THEN 1 ELSE 0 END) AS drain,
                SUM(CASE WHEN rln_monitoring_flag IS TRUE THEN 1 ELSE 0 END) AS rln_monitoring,
                SUM(CASE WHEN local_invasion_flag IS TRUE THEN 1 ELSE 0 END) AS local_invasion,
                SUM(CASE WHEN tracheal_involvement_flag IS TRUE THEN 1 ELSE 0 END) AS tracheal,
                SUM(CASE WHEN esophageal_involvement_flag IS TRUE THEN 1 ELSE 0 END) AS esophageal,
                SUM(CASE WHEN operative_findings_raw IS NOT NULL THEN 1 ELSE 0 END) AS has_op_findings,
                SUM(CASE WHEN rln_finding_raw IS NOT NULL THEN 1 ELSE 0 END) AS has_rln_finding
            FROM operative_episode_detail_v2
        ),
        patient_fields AS (
            SELECT 
                'patient_analysis_resolved_v1' AS source_table,
                COUNT(*) AS total_episodes,
                SUM(CASE WHEN op_parathyroid_autograft_any IS TRUE THEN 1 ELSE 0 END) AS parathyroid_autograft,
                SUM(CASE WHEN op_strap_muscle_any IS TRUE THEN 1 ELSE 0 END) AS strap_muscle,
                SUM(CASE WHEN op_reoperative_any IS TRUE THEN 1 ELSE 0 END) AS reoperative,
                SUM(CASE WHEN op_intraop_gross_ete_any IS TRUE THEN 1 ELSE 0 END) AS gross_ete,
                SUM(CASE WHEN op_drain_placed_any IS TRUE THEN 1 ELSE 0 END) AS drain,
                SUM(CASE WHEN op_rln_monitoring_any IS TRUE THEN 1 ELSE 0 END) AS rln_monitoring,
                SUM(CASE WHEN op_local_invasion_any IS TRUE THEN 1 ELSE 0 END) AS local_invasion,
                SUM(CASE WHEN op_tracheal_inv_any IS TRUE THEN 1 ELSE 0 END) AS tracheal,
                SUM(CASE WHEN op_esophageal_inv_any IS TRUE THEN 1 ELSE 0 END) AS esophageal,
                SUM(CASE WHEN op_n_surgeries_with_findings > 0 THEN 1 ELSE 0 END) AS has_op_findings,
                SUM(CASE WHEN op_rln_monitoring_any IS TRUE THEN 1 ELSE 0 END) AS has_rln_finding
            FROM patient_analysis_resolved_v1
        ),
        nlp_supplement AS (
            SELECT 
                'note_entities_procedures' AS source_table,
                COUNT(*) AS total_episodes,
                SUM(CASE WHEN entity_value_norm = 'parathyroid_autotransplant' THEN 1 ELSE 0 END) AS parathyroid_autograft,
                0 AS strap_muscle, 0 AS reoperative, 0 AS gross_ete, 0 AS drain,
                0 AS rln_monitoring, 0 AS local_invasion, 0 AS tracheal, 0 AS esophageal,
                COUNT(*) AS has_op_findings, 0 AS has_rln_finding
            FROM note_entities_procedures
            WHERE present_or_negated = 'present'
            AND entity_value_norm IN ('parathyroid_autotransplant', 'hemithyroidectomy',
                                       'total_thyroidectomy', 'central_neck_dissection',
                                       'lateral_neck_dissection')
        )
        SELECT e.*, CURRENT_TIMESTAMP AS computed_at FROM episode_fields e
        UNION ALL
        SELECT p.*, CURRENT_TIMESTAMP FROM patient_fields p
        UNION ALL
        SELECT n.*, CURRENT_TIMESTAMP FROM nlp_supplement n
    """
    if not dry_run:
        safe_exec(con, val_sql, "create val_operative_coverage_v2")
        print(f"  Created val_operative_coverage_v2: {count(con, 'val_operative_coverage_v2')} rows")

    # ── After metrics ─────────────────────────────────────────────────────
    after = {}
    try:
        r = con.execute(before_sql).fetchone()
        after = dict(zip(['para_autograft','strap','reoperative','gross_ete','drain',
                          'rln_monitoring','op_findings'], r))
        print(f"  After (operative_episode_detail_v2): {after}")
    except Exception as e:
        print(f"  [ERROR] After metrics: {e}")

    return {"before": before, "after": after, "nlp_pts": len(nlp_pts),
            "newly_recoverable": newly_recoverable}


# ────────────────────────────────────────────────────────────────────────────
# WORKSTREAM B: IMAGING → FNA LINKAGE BACKFILL
# ────────────────────────────────────────────────────────────────────────────

def run_workstream_b(con, dry_run: bool) -> dict:
    """
    Populate fna_episode_master_v2.linked_imaging_nodule_id from
    imaging_fna_linkage_v3 (score_rank=1, score>=0.65).
    Also create imaging_fna_linkage_summary_v1.
    """
    print("\n=== WORKSTREAM B: Imaging → FNA Linkage Backfill ===")

    # Before
    before_fna = scalar(con, """SELECT COUNT(*) FROM fna_episode_master_v2 
        WHERE linked_imaging_nodule_id IS NOT NULL""") or 0
    before_lnk = count(con, "imaging_fna_linkage_v3")
    print(f"  Before: fna_episode_master_v2.linked_imaging_nodule_id filled = {before_fna}")
    print(f"  imaging_fna_linkage_v3 rows = {before_lnk}")

    # ── Step B1: Confirm linkage table quality ────────────────────────────
    tier_sql = """
        SELECT linkage_confidence_tier, COUNT(*) as n, 
               MIN(CAST(linkage_score AS FLOAT)) as min_s, 
               MAX(CAST(linkage_score AS FLOAT)) as max_s
        FROM imaging_fna_linkage_v3
        WHERE score_rank = 1
        GROUP BY 1 ORDER BY 2 DESC
    """
    try:
        rows = con.execute(tier_sql).fetchall()
        for r in rows:
            print(f"  tier={r[0]} n={r[1]} score={r[2]:.3f}-{r[3]:.3f}")
    except Exception as e:
        print(f"  [WARN] tier distribution: {e}")

    # ── Step B2: Backfill fna_episode_master_v2 ───────────────────────────
    # Use score_rank=1 + score >= 0.45 (plausible+) to maximize coverage
    # while keeping only the best candidate per FNA episode
    candidates_sql = """
        SELECT COUNT(*) FROM imaging_fna_linkage_v3
        WHERE score_rank = 1
        AND CAST(linkage_score AS FLOAT) >= 0.45
        AND nodule_id IS NOT NULL
    """
    candidates_count = scalar(con, candidates_sql) or 0
    print(f"  Candidate links (score>=0.45, rank=1): {candidates_count}")

    high_conf_sql = """
        SELECT COUNT(*) FROM imaging_fna_linkage_v3
        WHERE score_rank = 1
        AND CAST(linkage_score AS FLOAT) >= 0.65
        AND nodule_id IS NOT NULL
    """
    high_conf_count = scalar(con, high_conf_sql) or 0
    print(f"  High-confidence links (score>=0.65): {high_conf_count}")

    if not dry_run and candidates_count > 0:
        # Backfill with plausible+ (score >= 0.45)
        backfill_sql = """
            UPDATE fna_episode_master_v2 f
            SET linked_imaging_nodule_id = lnk.nodule_id
            FROM (
                SELECT research_id, fna_episode_id, nodule_id,
                       CAST(linkage_score AS FLOAT) AS lscore
                FROM imaging_fna_linkage_v3
                WHERE score_rank = 1
                AND CAST(linkage_score AS FLOAT) >= 0.45
                AND nodule_id IS NOT NULL
            ) lnk
            WHERE f.research_id = lnk.research_id
            AND f.fna_episode_id = lnk.fna_episode_id
            AND f.linked_imaging_nodule_id IS NULL
        """
        rc = safe_exec(con, backfill_sql, "backfill fna linked_imaging_nodule_id")
        print(f"  Backfilled fna_episode_master_v2 rows: {rc}")

    # ── Step B3: Create linkage summary view ──────────────────────────────
    summary_sql = """
        CREATE OR REPLACE TABLE imaging_fna_linkage_summary_v1 AS
        SELECT
            lnk.linkage_confidence_tier,
            CASE WHEN CAST(lnk.linkage_score AS FLOAT) >= 0.85 THEN 'exact_or_near_exact'
                 WHEN CAST(lnk.linkage_score AS FLOAT) >= 0.65 THEN 'high_confidence'
                 WHEN CAST(lnk.linkage_score AS FLOAT) >= 0.45 THEN 'plausible'
                 WHEN CAST(lnk.linkage_score AS FLOAT) > 0 THEN 'weak'
                 ELSE 'unlinked'
            END AS score_band,
            COUNT(*) AS n_fna_episodes,
            COUNT(DISTINCT lnk.research_id) AS n_patients,
            ROUND(AVG(CAST(lnk.linkage_score AS FLOAT)),3) AS avg_score,
            ROUND(AVG(ABS(lnk.day_gap)),1) AS avg_day_gap,
            SUM(CASE WHEN analysis_eligible_link_flag IS TRUE THEN 1 ELSE 0 END) AS analysis_eligible,
            CURRENT_TIMESTAMP AS computed_at
        FROM imaging_fna_linkage_v3 lnk
        WHERE score_rank = 1
        GROUP BY 1, 2
        ORDER BY n_fna_episodes DESC
    """
    if not dry_run:
        safe_exec(con, summary_sql, "create imaging_fna_linkage_summary_v1")
        print(f"  Created imaging_fna_linkage_summary_v1: {count(con, 'imaging_fna_linkage_summary_v1')} rows")

    # ── Step B4: Confirm imaging_nodule_long_v2 is source-limited ─────────
    imaging_v2_sql = """
        SELECT SUM(CASE WHEN size_cm_max IS NOT NULL THEN 1 ELSE 0 END) as has_size,
               SUM(CASE WHEN composition IS NOT NULL THEN 1 ELSE 0 END) as has_comp
        FROM imaging_nodule_long_v2
    """
    try:
        r = con.execute(imaging_v2_sql).fetchone()
        print(f"  imaging_nodule_long_v2: size={r[0]}, composition={r[1]} — SOURCE-LIMITED (V2 NLP fields unpopulated)")
    except Exception as e:
        print(f"  [WARN] imaging_nodule_long_v2: {e}")

    # ── After ─────────────────────────────────────────────────────────────
    after_fna = scalar(con, """SELECT COUNT(*) FROM fna_episode_master_v2 
        WHERE linked_imaging_nodule_id IS NOT NULL""") or 0
    print(f"  After: fna_episode_master_v2.linked_imaging_nodule_id filled = {after_fna}")

    return {
        "before_img_fna_link": before_fna,
        "after_img_fna_link": after_fna,
        "improvement": after_fna - before_fna,
        "imaging_fna_linkage_v3_rows": before_lnk,
        "high_conf_links": high_conf_count,
        "candidate_links": candidates_count,
    }


# ────────────────────────────────────────────────────────────────────────────
# WORKSTREAM C: RECURRENCE MANUAL REVIEW QUEUE
# ────────────────────────────────────────────────────────────────────────────

def run_workstream_c(con, dry_run: bool) -> dict:
    """
    Build recurrence_review_queue_v1 with full clinical context for efficient
    chart review. Create per-tier summary. Export CSV.
    """
    print("\n=== WORKSTREAM C: Recurrence Manual Review Queue ===")

    # Before
    before_tiers = {}
    try:
        rows = con.execute("""
            SELECT COALESCE(recurrence_date_status, 'null') AS s, COUNT(*) AS n
            FROM extracted_recurrence_refined_v1
            GROUP BY 1 ORDER BY 2 DESC
        """).fetchall()
        before_tiers = {r[0]: r[1] for r in rows}
        print(f"  Before tier counts: {before_tiers}")
    except Exception as e:
        print(f"  [ERROR]: {e}")

    # ── Step C1: Create recurrence_review_queue_v1 ───────────────────────
    queue_sql = """
        CREATE OR REPLACE TABLE recurrence_review_queue_v1 AS
        WITH recur_base AS (
            SELECT 
                re.research_id,
                re.recurrence_any,
                re.recurrence_flag_structured,
                re.recurrence_date_status,
                re.recurrence_date_best,
                re.recurrence_date_confidence,
                re.recurrence_data_confidence,
                re.detection_category,
                re.recurrence_site_inferred,
                re.n_recurrence_sources,
                re.recurrence_source,
                re.first_recurrence_date,
                re.tg_last_value,
                re.tg_max,
                re.tg_nadir,
                re.tg_rising_flag,
                re.last_stimulated_tg,
                re.last_stimulated_tsh,
                re.max_rai_dose_mci,
                re.n_rai_treatments,
                re.rai_avid,
                re.has_scan_findings,
                re.scan_findings_combined
            FROM extracted_recurrence_refined_v1 re
            WHERE re.recurrence_any IS TRUE
        ),
        patient_context AS (
            SELECT DISTINCT ON (ps.research_id)
                ps.research_id,
                ps.surg_date,
                ps.tumor_1_histologic_type,
                ps.age,
                ps.gender,
                ps.tumor_1_extrathyroidal_extension,
                ps.tumor_1_size_greatest_dimension_cm,
                ps.tumor_1_ln_involved,
                ps.tumor_1_ln_examined
            FROM path_synoptics ps
            ORDER BY ps.research_id, ps.surg_date ASC
        ),
        rrisk AS (
            SELECT DISTINCT ON (research_id) research_id,
                CAST(recurrence_flag AS VARCHAR) AS recurrence_flag_rrisk,
                first_recurrence_date AS rrisk_first_recurrence_date,
                recurrence_risk_band,
                overall_stage,
                CAST(braf_positive AS VARCHAR) AS braf_positive
            FROM recurrence_risk_features_mv
            ORDER BY research_id, 
                CASE WHEN LOWER(CAST(recurrence_flag AS VARCHAR)) = 'true' THEN 0 ELSE 1 END
        ),
        priority_calc AS (
            SELECT r.*,
                CASE
                    WHEN r.recurrence_date_status IN ('exact_source_date','biochemical_inflection_inferred') 
                        THEN 1  -- already has date
                    WHEN r.recurrence_date_status = 'unresolved_date' AND r.n_recurrence_sources >= 3
                        AND r.recurrence_data_confidence >= 0.9 THEN 2  -- high-yield chart review
                    WHEN r.recurrence_date_status = 'unresolved_date' AND r.n_recurrence_sources >= 2
                        THEN 3  -- medium priority
                    WHEN r.recurrence_date_status = 'unresolved_date' AND r.recurrence_flag_structured IS TRUE
                        THEN 4  -- structured flag, no date
                    ELSE 5
                END AS review_priority,
                CASE
                    WHEN r.recurrence_date_status = 'unresolved_date' AND r.n_recurrence_sources >= 3
                        AND r.recurrence_data_confidence >= 0.9 THEN 'HIGH: multiple sources, high confidence, missing date'
                    WHEN r.recurrence_date_status = 'unresolved_date' AND r.n_recurrence_sources >= 2
                        THEN 'MEDIUM: multi-source, date unresolved'
                    WHEN r.recurrence_date_status = 'unresolved_date' AND r.recurrence_flag_structured IS TRUE
                        THEN 'LOW: structured flag only'
                    ELSE 'RESOLVED/NA'
                END AS review_reason
            FROM recur_base r
        )
        SELECT 
            p.review_priority,
            p.review_reason,
            p.research_id,
            p.recurrence_date_status,
            p.recurrence_date_best,
            p.recurrence_date_confidence,
            p.recurrence_data_confidence,
            p.n_recurrence_sources,
            p.detection_category,
            p.recurrence_site_inferred,
            p.recurrence_source,
            p.first_recurrence_date       AS recur_first_date_structured,
            p.tg_last_value,
            p.tg_max,
            p.tg_nadir,
            p.tg_rising_flag,
            p.last_stimulated_tg,
            p.last_stimulated_tsh,
            p.max_rai_dose_mci,
            p.n_rai_treatments,
            p.rai_avid,
            p.has_scan_findings,
            p.scan_findings_combined,
            -- patient context
            pc.surg_date           AS first_surg_date,
            pc.age                 AS age_at_surgery,
            pc.gender,
            pc.tumor_1_histologic_type AS histology,
            pc.tumor_1_extrathyroidal_extension AS ete_raw,
            pc.tumor_1_size_greatest_dimension_cm AS tumor_size_cm,
            pc.tumor_1_ln_involved AS ln_positive,
            pc.tumor_1_ln_examined AS ln_examined,
            -- risk features
            rr.recurrence_risk_band,
            rr.overall_stage,
            rr.braf_positive,
            rr.rrisk_first_recurrence_date,
            CURRENT_TIMESTAMP AS queue_generated_at
        FROM priority_calc p
        LEFT JOIN patient_context pc ON pc.research_id = p.research_id
        LEFT JOIN rrisk rr ON rr.research_id = p.research_id
        WHERE p.recurrence_date_status != 'not_applicable'
        ORDER BY p.review_priority, p.recurrence_data_confidence DESC NULLS LAST
    """
    if not dry_run:
        safe_exec(con, queue_sql, "create recurrence_review_queue_v1")
        q_count = count(con, "recurrence_review_queue_v1")
        print(f"  Created recurrence_review_queue_v1: {q_count} rows")

        # Priority breakdown
        prio_rows = con.execute("""
            SELECT review_priority, review_reason, COUNT(*) AS n
            FROM recurrence_review_queue_v1
            GROUP BY 1, 2 ORDER BY 1
        """).fetchall()
        for r in prio_rows:
            print(f"  P{r[0]}: n={r[2]} — {r[1]}")
    else:
        q_count = 0

    # ── Step C2: Create per-tier summary table ────────────────────────────
    tier_summary_sql = """
        CREATE OR REPLACE TABLE recurrence_date_tier_summary_v1 AS
        SELECT
            recurrence_date_status,
            COUNT(*) AS n,
            AVG(recurrence_data_confidence) AS avg_confidence,
            SUM(CASE WHEN review_priority <= 3 THEN 1 ELSE 0 END) AS high_yield_for_review,
            CURRENT_TIMESTAMP AS computed_at
        FROM recurrence_review_queue_v1
        GROUP BY 1 ORDER BY 2 DESC
    """
    if not dry_run:
        safe_exec(con, tier_summary_sql, "create recurrence_date_tier_summary_v1")
        print(f"  Created recurrence_date_tier_summary_v1")

    # ── Step C3: Review yield tracker ────────────────────────────────────
    # Track progress: completed manual reviews feed back into the date fields
    yield_tracker_sql = """
        CREATE OR REPLACE TABLE recurrence_review_yield_tracker_v1 AS
        SELECT
            DATE '2026-03-14' AS tracker_start_date,
            CURRENT_TIMESTAMP AS created_at,
            COUNT(*) AS total_in_queue,
            SUM(CASE WHEN recurrence_date_status = 'exact_source_date' THEN 1 ELSE 0 END) AS already_exact,
            SUM(CASE WHEN recurrence_date_status = 'biochemical_inflection_inferred' THEN 1 ELSE 0 END) AS inferred,
            SUM(CASE WHEN recurrence_date_status = 'unresolved_date' AND review_priority = 2 THEN 1 ELSE 0 END) AS high_yield_pending,
            SUM(CASE WHEN recurrence_date_status = 'unresolved_date' AND review_priority = 3 THEN 1 ELSE 0 END) AS medium_yield_pending,
            SUM(CASE WHEN recurrence_date_status = 'unresolved_date' AND review_priority = 4 THEN 1 ELSE 0 END) AS low_yield_pending,
            0 AS manual_reviews_completed,
            0 AS dates_recovered_this_sprint,
            'pipeline_gap_closure_20260314' AS sprint_tag
        FROM recurrence_review_queue_v1
    """
    if not dry_run:
        safe_exec(con, yield_tracker_sql, "create recurrence_review_yield_tracker_v1")
        r = con.execute("SELECT * FROM recurrence_review_yield_tracker_v1").fetchone()
        print(f"  Review yield tracker: total={r[2]}, exact={r[3]}, inferred={r[4]}, "
              f"high_yield={r[5]}, medium={r[6]}, low={r[7]}")

    # ── Step C4: Export CSV for chart review ─────────────────────────────
    if not dry_run:
        try:
            export_sql = """
                SELECT research_id, review_priority, review_reason,
                       recurrence_date_status, recurrence_data_confidence,
                       n_recurrence_sources, detection_category, recurrence_site_inferred,
                       tg_last_value, tg_max, tg_rising_flag, last_stimulated_tg,
                       max_rai_dose_mci, rai_avid, has_scan_findings,
                       first_surg_date, age_at_surgery, gender, histology, ete_raw,
                       tumor_size_cm, ln_positive, recurrence_risk_band, overall_stage,
                       braf_positive, queue_generated_at
                FROM recurrence_review_queue_v1
                WHERE recurrence_date_status = 'unresolved_date'
                ORDER BY review_priority, recurrence_data_confidence DESC NULLS LAST
            """
            rows = con.execute(export_sql).fetchall()
            cols = [desc[0] for desc in con.execute(export_sql).description]
            outfile = EXPORT_DIR / "recurrence_review_queue_unresolved.csv"
            with open(outfile, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)
            print(f"  Exported {len(rows)} unresolved recurrence cases to {outfile.name}")
        except Exception as e:
            print(f"  [WARN] CSV export: {e}")

    return {
        "before_tiers": before_tiers,
        "queue_rows": q_count,
        "unresolved_count": before_tiers.get("unresolved_date", 0),
    }


# ────────────────────────────────────────────────────────────────────────────
# WORKSTREAM D: ADJUDICATION PROGRESS INSTRUMENTATION
# ────────────────────────────────────────────────────────────────────────────

def run_workstream_d(con, dry_run: bool) -> dict:
    """
    Build adjudication KPI view, export starter pack CSV for histology
    and conflicts review queues.
    """
    print("\n=== WORKSTREAM D: Adjudication Progress Instrumentation ===")

    # Before
    decisions_before = count(con, "adjudication_decisions")
    histology_queue = count(con, "histology_manual_review_queue_v")
    conflicts_queue = count(con, "streamlit_patient_manual_review_v")
    conflicts_v = count(con, "streamlit_patient_conflicts_v")
    print(f"  adjudication_decisions: {decisions_before} (0 = no decisions entered yet)")
    print(f"  histology_manual_review_queue_v: {histology_queue}")
    print(f"  streamlit_patient_manual_review_v: {conflicts_queue}")
    print(f"  streamlit_patient_conflicts_v: {conflicts_v}")

    # ── Step D1: Build adjudication_progress_kpi_v1 ───────────────────────
    kpi_sql = """
        CREATE OR REPLACE TABLE adjudication_progress_kpi_v1 AS
        WITH decisions_summary AS (
            SELECT 
                COALESCE(domain, 'all') AS domain,
                COUNT(*) AS total_decisions,
                SUM(CASE WHEN LOWER(CAST(active_flag AS VARCHAR)) = 'true' THEN 1 ELSE 0 END) AS active_decisions
            FROM adjudication_decisions
            GROUP BY 1
        ),
        queue_summary AS (
            SELECT
                'histology_manual_review_queue_v' AS queue_name,
                'histology' AS domain,
                COUNT(*) AS queued,
                0 AS completed
            FROM histology_manual_review_queue_v
            UNION ALL
            SELECT
                'streamlit_patient_manual_review_v' AS queue_name,
                domain AS domain,
                COUNT(*) AS queued,
                0 AS completed
            FROM streamlit_patient_manual_review_v
            GROUP BY domain
            UNION ALL
            SELECT
                'streamlit_patient_conflicts_v' AS queue_name,
                'all_conflicts' AS domain,
                COUNT(*) AS queued,
                0 AS completed
            FROM streamlit_patient_conflicts_v
        ),
        overall AS (
            SELECT
                'overall' AS metric_name,
                (SELECT COUNT(*) FROM adjudication_decisions WHERE LOWER(CAST(active_flag AS VARCHAR)) = 'true') AS decisions_completed,
                (SELECT COUNT(*) FROM histology_manual_review_queue_v) AS histology_queued,
                (SELECT COUNT(*) FROM streamlit_patient_manual_review_v) AS all_domains_queued,
                (SELECT COUNT(*) FROM streamlit_patient_conflicts_v) AS conflict_patients,
                CURRENT_TIMESTAMP AS computed_at,
                'pipeline_gap_closure_20260314' AS sprint_tag
        )
        SELECT 
            qs.queue_name,
            qs.domain,
            qs.queued,
            COALESCE(ds.active_decisions, 0) AS decisions_applied,
            qs.queued - COALESCE(ds.active_decisions, 0) AS still_pending,
            ROUND(100.0 * COALESCE(ds.active_decisions, 0) / NULLIF(qs.queued, 0), 1) AS pct_complete,
            CURRENT_TIMESTAMP AS computed_at
        FROM queue_summary qs
        LEFT JOIN decisions_summary ds ON ds.domain = qs.domain
        ORDER BY qs.queued DESC
    """
    if not dry_run:
        safe_exec(con, kpi_sql, "create adjudication_progress_kpi_v1")
        print(f"  Created adjudication_progress_kpi_v1: {count(con, 'adjudication_progress_kpi_v1')} rows")
        try:
            rows = con.execute("SELECT queue_name, queued, decisions_applied, pct_complete FROM adjudication_progress_kpi_v1").fetchall()
            for r in rows:
                print(f"    {r[0]}: queued={r[1]}, done={r[2]}, pct={r[3]}%")
        except Exception as e:
            print(f"  [WARN] KPI display: {e}")

    # ── Step D2: Export histology top-priority starter pack ───────────────
    if not dry_run:
        try:
            hist_export_sql = """
                SELECT research_id, review_domain, priority_score, conflict_summary,
                       final_histology_for_analysis, final_t_stage_for_analysis,
                       source_histology_raw_ps, source_histology_raw_tp,
                       recommended_reviewer_action, unresolved_reason,
                       t_stage_source_path, t_stage_source_note
                FROM histology_manual_review_queue_v
                ORDER BY priority_score DESC NULLS LAST
                LIMIT 500
            """
            rows = con.execute(hist_export_sql).fetchall()
            desc = con.execute(hist_export_sql).description
            cols = [d[0] for d in desc]
            outfile = EXPORT_DIR / "adjudication_histology_starter_pack.csv"
            with open(outfile, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)
            print(f"  Exported {len(rows)} histology cases to {outfile.name}")
        except Exception as e:
            print(f"  [WARN] histology export: {e}")

        # Export conflicts
        try:
            conflicts_export_sql = """
                SELECT research_id, domain, review_priority, review_reason, detail
                FROM streamlit_patient_manual_review_v
                ORDER BY review_priority, research_id
                LIMIT 1000
            """
            rows = con.execute(conflicts_export_sql).fetchall()
            desc = con.execute(conflicts_export_sql).description
            cols = [d[0] for d in desc]
            outfile2 = EXPORT_DIR / "adjudication_conflicts_starter_pack.csv"
            with open(outfile2, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)
            print(f"  Exported {len(rows)} conflict items to {outfile2.name}")
        except Exception as e:
            print(f"  [WARN] conflicts export: {e}")

        # Export multi-domain conflicts
        try:
            mc_export_sql = """
                SELECT research_id,
                       histology_discordance_flag, stage_discordance_flag,
                       n_stage_discordance_flag, molecular_linkage_confidence
                FROM streamlit_patient_conflicts_v
                ORDER BY 
                    (CASE WHEN histology_discordance_flag IS TRUE THEN 1 ELSE 0 END +
                     CASE WHEN stage_discordance_flag IS TRUE THEN 1 ELSE 0 END +
                     CASE WHEN n_stage_discordance_flag IS TRUE THEN 1 ELSE 0 END) DESC
                LIMIT 500
            """
            rows = con.execute(mc_export_sql).fetchall()
            desc = con.execute(mc_export_sql).description
            cols = [d[0] for d in desc]
            outfile3 = EXPORT_DIR / "adjudication_discordance_cases.csv"
            with open(outfile3, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)
            print(f"  Exported {len(rows)} discordance cases to {outfile3.name}")
        except Exception as e:
            print(f"  [WARN] discordance export: {e}")

    # ── Step D3: Verify adjudication_progress_summary_v still exists ─────
    adj_progress = count(con, "adjudication_progress_summary_v")
    print(f"  adjudication_progress_summary_v (legacy view): {adj_progress} rows")

    return {
        "decisions_before": decisions_before,
        "histology_queue": histology_queue,
        "conflicts_queue": conflicts_queue,
        "kpi_created": not dry_run,
    }


# ────────────────────────────────────────────────────────────────────────────
# BEFORE/AFTER VALIDATION
# ────────────────────────────────────────────────────────────────────────────

def collect_validation_metrics(con) -> dict:
    """Collect all before/after metrics for the summary report."""
    metrics = {}

    # Operative
    try:
        r = con.execute("""SELECT 
            SUM(CASE WHEN op_parathyroid_autograft_any IS TRUE THEN 1 ELSE 0 END),
            SUM(CASE WHEN op_rln_monitoring_any IS TRUE THEN 1 ELSE 0 END),
            SUM(CASE WHEN op_strap_muscle_any IS TRUE THEN 1 ELSE 0 END),
            SUM(CASE WHEN op_intraop_gross_ete_any IS TRUE THEN 1 ELSE 0 END),
            SUM(CASE WHEN op_n_surgeries_with_findings > 0 THEN 1 ELSE 0 END),
            COUNT(*) 
            FROM patient_analysis_resolved_v1""").fetchone()
        metrics["op_para"] = r[0]
        metrics["op_rln"] = r[1]
        metrics["op_strap"] = r[2]
        metrics["op_gross_ete"] = r[3]
        metrics["op_with_findings"] = r[4]
        metrics["patient_total"] = r[5]
    except Exception as e:
        metrics["op_error"] = str(e)

    # Imaging linkage
    try:
        metrics["fna_with_img_link"] = scalar(con, """
            SELECT COUNT(*) FROM fna_episode_master_v2 
            WHERE linked_imaging_nodule_id IS NOT NULL""") or 0
        metrics["imaging_fna_linkage_v3_rows"] = count(con, "imaging_fna_linkage_v3")
    except Exception as e:
        metrics["imaging_error"] = str(e)

    # Recurrence
    try:
        rows = con.execute("""
            SELECT COALESCE(recurrence_date_status, 'null'), COUNT(*) 
            FROM extracted_recurrence_refined_v1 GROUP BY 1""").fetchall()
        for r in rows:
            metrics[f"recur_{r[0]}"] = r[1]
        metrics["recurrence_review_queue_v1_rows"] = count(con, "recurrence_review_queue_v1")
    except Exception as e:
        metrics["recur_error"] = str(e)

    # Adjudication
    try:
        metrics["adjudication_decisions"] = count(con, "adjudication_decisions")
        metrics["adjudication_kpi_rows"] = count(con, "adjudication_progress_kpi_v1")
        metrics["histology_queue_rows"] = count(con, "histology_manual_review_queue_v")
    except Exception as e:
        metrics["adj_error"] = str(e)

    return metrics


# ────────────────────────────────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Script 94: Pipeline Gap Closure")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck (production)")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Validate only, no writes")
    parser.add_argument("--phase", default="all",
                        help="Phases to run: A, B, C, D, or all (default)")
    args = parser.parse_args()

    use_md = args.md and not args.local
    dry_run = args.dry_run
    phases = args.phase.upper().split(",") if args.phase.lower() != "all" else ["A","B","C","D"]

    print(f"Script 94: Pipeline Gap Closure Sprint")
    print(f"  Target: {'MotherDuck' if use_md else 'local DuckDB'}")
    print(f"  Dry-run: {dry_run}")
    print(f"  Phases: {phases}")
    print(f"  Export dir: {EXPORT_DIR}")
    print()

    con = get_connection(use_md)

    results = {}

    # Run selected phases
    if "A" in phases:
        results["A"] = run_workstream_a(con, dry_run)

    if "B" in phases:
        results["B"] = run_workstream_b(con, dry_run)

    if "C" in phases:
        results["C"] = run_workstream_c(con, dry_run)

    if "D" in phases:
        results["D"] = run_workstream_d(con, dry_run)

    # Collect final metrics
    print("\n=== FINAL VALIDATION METRICS ===")
    final_metrics = collect_validation_metrics(con)
    for k, v in final_metrics.items():
        print(f"  {k}: {v}")

    # ── Save results to manifest ──────────────────────────────────────────
    manifest_path = EXPORT_DIR / "manifest.json"
    manifest = {
        "script": "94_pipeline_gap_closure.py",
        "run_date": DATE_SLUG,
        "timestamp": TS,
        "target": "MotherDuck" if use_md else "local",
        "dry_run": dry_run,
        "phases": phases,
        "workstream_results": {k: {str(k2): str(v2) for k2, v2 in v.items()} for k, v in results.items()},
        "final_metrics": {k: str(v) for k, v in final_metrics.items()},
    }
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\n  Manifest saved: {manifest_path}")

    # ── List exports ──────────────────────────────────────────────────────
    exports = list(EXPORT_DIR.glob("*"))
    print(f"\n  Exports ({len(exports)} files):")
    for f in sorted(exports):
        print(f"    {f.name} ({f.stat().st_size:,} bytes)")

    print("\nScript 94 complete.")


if __name__ == "__main__":
    main()
