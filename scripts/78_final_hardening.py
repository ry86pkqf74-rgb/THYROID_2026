#!/usr/bin/env python3
"""
78_final_hardening.py -- Final hardening pass

Phases:
  A: Recurrence manual review queue + validation summary
  B: Imaging-FNA linkage re-run (relaxed UNION preferring v1 features)
  C: RAI dose missingness classification
  D: Lab canonical contract validation

Supports --md (MotherDuck), --local, --dry-run, --phase A/B/C/D/all.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
sys.path.insert(0, str(ROOT))

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")


def section(title: str) -> None:
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}\n")


def safe_exec(con: duckdb.DuckDBPyConnection, sql: str, dry: bool = False) -> int:
    if dry:
        print(f"  [DRY RUN] {sql[:120]}...")
        return 0
    try:
        r = con.execute(sql)
        ct = r.fetchone()
        return int(ct[0]) if ct else 0
    except Exception:
        con.execute(sql)
        return -1


def safe_count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    try:
        r = con.execute(sql).fetchone()
        return int(r[0]) if r else 0
    except Exception:
        return -1


def table_exists(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def connect(args) -> duckdb.DuckDBPyConnection:
    if args.md:
        import toml
        token = toml.load(str(ROOT / ".streamlit" / "secrets.toml"))["MOTHERDUCK_TOKEN"]
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(str(DB_PATH))


# ─────────────────────────────────────────────────────────────────────────────
# Phase A: Recurrence manual review queue + validation summary
# ─────────────────────────────────────────────────────────────────────────────

RECURRENCE_REVIEW_QUEUE_SQL = """
CREATE OR REPLACE TABLE recurrence_manual_review_queue_v1 AS
WITH recur AS (
    SELECT
        r.research_id,
        r.recurrence_any,
        r.recurrence_flag_structured,
        r.detection_category,
        r.recurrence_site_inferred,
        r.recurrence_date_status,
        r.recurrence_date_best,
        r.recurrence_date_confidence,
        r.tg_rising_flag,
        r.tg_nadir,
        r.tg_max
    FROM extracted_recurrence_refined_v1 r
    WHERE r.recurrence_date_status = 'unresolved_date'
),
patient_ctx AS (
    SELECT DISTINCT
        ps.research_id,
        ps.surg_date,
        pls.histology_1_type,
        pls.overall_stage_ajcc8,
        pls.sex,
        pls.age_at_surgery AS age
    FROM path_synoptics ps
    LEFT JOIN patient_level_summary_mv pls
        ON ps.research_id = pls.research_id
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ps.research_id ORDER BY ps.surg_date
    ) = 1
),
manuscript_flag AS (
    SELECT DISTINCT research_id, TRUE AS in_manuscript_cohort
    FROM manuscript_cohort_v1
)
SELECT
    recur.research_id,
    COALESCE(recur.recurrence_any, FALSE)       AS recurrence_present,
    recur.detection_category,
    recur.recurrence_site_inferred              AS evidence_source,
    pc.histology_1_type                         AS cancer_histology,
    pc.overall_stage_ajcc8                      AS ajcc_stage,
    TRY_CAST(pc.surg_date AS DATE)             AS surgery_date_anchor,
    COALESCE(mf.in_manuscript_cohort, FALSE)    AS in_manuscript_cohort,
    recur.recurrence_date_status                AS date_status,
    recur.tg_rising_flag,
    recur.tg_nadir,
    recur.tg_max,
    -- Priority scoring
    (CASE WHEN mf.in_manuscript_cohort THEN 10 ELSE 0 END
     + CASE WHEN recur.detection_category = 'structural_confirmed' THEN 5
            WHEN recur.detection_category = 'structural_date_unknown' THEN 3
            ELSE 0 END
     + CASE WHEN recur.tg_rising_flag IS TRUE THEN 2 ELSE 0 END
     + CASE WHEN pc.histology_1_type IN ('PTC','FTC','MTC','PDTC','ATC','HCC') THEN 2 ELSE 0 END
    )                                           AS priority_score,
    CASE
        WHEN recur.detection_category = 'structural_confirmed'
            THEN 'Structural recurrence confirmed but no date extractable from any NLP or structured source'
        WHEN recur.detection_category = 'structural_date_unknown'
            THEN 'Structural recurrence suspected but date and confirmation both unresolved'
        WHEN recur.tg_rising_flag IS TRUE
            THEN 'Biochemical recurrence (rising Tg) but inflection date imprecise'
        ELSE 'Recurrence evidence present but unclassifiable date'
    END                                         AS missingness_reason,
    'extracted_recurrence_refined_v1'           AS source_table
FROM recur
LEFT JOIN patient_ctx pc ON recur.research_id = pc.research_id
LEFT JOIN manuscript_flag mf ON recur.research_id = mf.research_id
ORDER BY priority_score DESC, recur.research_id
"""

VAL_RECURRENCE_DATE_RESOLUTION_SQL = """
CREATE OR REPLACE TABLE val_recurrence_date_resolution_v1 AS
SELECT
    recurrence_date_status                      AS date_tier,
    COUNT(*)                                    AS n_rows,
    COUNT(DISTINCT research_id)                 AS n_patients,
    COUNT(*) FILTER (
        WHERE research_id IN (SELECT research_id FROM manuscript_cohort_v1)
    )                                           AS n_in_manuscript_cohort,
    ROUND(AVG(recurrence_date_confidence), 2)   AS avg_confidence,
    CURRENT_TIMESTAMP                           AS audited_at
FROM extracted_recurrence_refined_v1
GROUP BY recurrence_date_status
ORDER BY
    CASE recurrence_date_status
        WHEN 'exact_source_date' THEN 1
        WHEN 'biochemical_inflection_inferred' THEN 2
        WHEN 'unresolved_date' THEN 3
        WHEN 'not_applicable' THEN 4
        ELSE 5
    END
"""


def phase_a(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase A: Recurrence Review Queue + Validation")
    results: dict = {}

    if not table_exists(con, "extracted_recurrence_refined_v1"):
        print("  SKIP: extracted_recurrence_refined_v1 not found")
        return results

    before_queue = safe_count(
        con, "SELECT COUNT(*) FROM recurrence_manual_review_queue_v1"
    ) if table_exists(con, "recurrence_manual_review_queue_v1") else 0

    safe_exec(con, RECURRENCE_REVIEW_QUEUE_SQL, dry)
    after_queue = safe_count(
        con, "SELECT COUNT(*) FROM recurrence_manual_review_queue_v1"
    )
    print(f"  recurrence_manual_review_queue_v1: {before_queue} -> {after_queue} rows")
    results["recurrence_review_queue_rows"] = after_queue

    if table_exists(con, "manuscript_cohort_v1"):
        mc_count = safe_count(
            con,
            "SELECT COUNT(*) FROM recurrence_manual_review_queue_v1 "
            "WHERE in_manuscript_cohort"
        )
        print(f"  ... of which {mc_count} are in manuscript cohort")
        results["recurrence_queue_manuscript"] = mc_count

    safe_exec(con, VAL_RECURRENCE_DATE_RESOLUTION_SQL, dry)
    val_rows = safe_count(
        con, "SELECT COUNT(*) FROM val_recurrence_date_resolution_v1"
    )
    print(f"  val_recurrence_date_resolution_v1: {val_rows} tier rows")
    results["val_recurrence_tiers"] = val_rows

    if not dry:
        tiers = con.execute(
            "SELECT * FROM val_recurrence_date_resolution_v1"
        ).fetchdf()
        print("\n  Date Resolution Summary:")
        for _, row in tiers.iterrows():
            print(f"    {row['date_tier']:<40} "
                  f"n={int(row['n_rows']):>6}  "
                  f"manuscript={int(row['n_in_manuscript_cohort']):>5}")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Phase B: Imaging-FNA linkage re-run with relaxed UNION
# ─────────────────────────────────────────────────────────────────────────────

def _get_scoring_helpers() -> dict:
    """Import scoring helper SQL fragments from script 49."""
    try:
        sys.path.insert(0, str(ROOT / "scripts"))
        from importlib import import_module
        mod = import_module("49_enhanced_linkage_v3")
        return {
            "temporal": mod.temporal_score_sql(
                "ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native))"
            ),
            "laterality": mod.laterality_score_sql("img.laterality", "fna.fna_lat"),
            "size": mod.size_compat_score_sql("img.size_cm_max", "fna.fna_nodule_size_cm"),
            "penalty": mod.ambiguity_penalty_sql(
                "COUNT(*) OVER (PARTITION BY research_id, nodule_id)"
            ),
            "tier": mod.confidence_tier_from_score_sql("linkage_score"),
        }
    except Exception as e:
        print(f"  WARN: Could not import script 49 helpers: {e}")
        print("  Using inline scoring SQL fallback")
        return {
            "temporal": """CASE
                WHEN ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) = 0 THEN 1.0
                WHEN ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) <= 7 THEN 0.9
                WHEN ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) <= 30 THEN 0.7
                WHEN ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) <= 90 THEN 0.5
                WHEN ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) <= 180 THEN 0.3
                WHEN ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) <= 365 THEN 0.1
                ELSE 0.0 END""",
            "laterality": """CASE
                WHEN img.laterality = fna.fna_lat THEN 1.0
                WHEN img.laterality IS NULL OR fna.fna_lat IS NULL THEN 0.5
                ELSE 0.0 END""",
            "size": """CASE
                WHEN img.size_cm_max IS NULL OR fna.fna_nodule_size_cm IS NULL THEN 0.5
                WHEN ABS(img.size_cm_max - fna.fna_nodule_size_cm) <= 0.5 THEN 1.0
                WHEN ABS(img.size_cm_max - fna.fna_nodule_size_cm) <= 1.0 THEN 0.7
                WHEN ABS(img.size_cm_max - fna.fna_nodule_size_cm) <= 2.0 THEN 0.3
                ELSE 0.1 END""",
            "penalty": """CASE
                WHEN COUNT(*) OVER (PARTITION BY research_id, nodule_id) = 1 THEN 0.0
                WHEN COUNT(*) OVER (PARTITION BY research_id, nodule_id) = 2 THEN 0.1
                ELSE 0.2 END""",
            "tier": """CASE
                WHEN linkage_score >= 0.85 THEN 'exact_match'
                WHEN linkage_score >= 0.65 THEN 'high_confidence'
                WHEN linkage_score >= 0.45 THEN 'plausible'
                WHEN linkage_score > 0 THEN 'weak'
                ELSE 'unlinked' END""",
        }


IMAGING_FNA_RELAXED_SQL_TEMPLATE = """
CREATE OR REPLACE TABLE imaging_fna_linkage_v3 AS
WITH v1_img AS (
    SELECT research_id, nodule_id, exam_id AS imaging_exam_id,
           exam_date AS exam_date_native, laterality, max_dimension_cm AS size_cm_max
    FROM imaging_nodule_master_v1
    WHERE exam_date IS NOT NULL
),
v2_img AS (
    SELECT research_id, nodule_id, imaging_exam_id,
           exam_date_native, laterality, size_cm_max
    FROM imaging_nodule_long_v2
    WHERE exam_date_native IS NOT NULL
      AND research_id NOT IN (SELECT DISTINCT research_id FROM v1_img)
),
img AS (
    SELECT * FROM v1_img
    UNION ALL
    SELECT * FROM v2_img
),
fna AS (
    SELECT research_id, fna_episode_id, fna_date_native,
           laterality AS fna_lat, bethesda_category, specimen_site_raw,
           NULL::DOUBLE AS fna_nodule_size_cm
    FROM fna_episode_master_v2
    WHERE fna_date_native IS NOT NULL
),
candidates AS (
    SELECT
        img.research_id,
        img.nodule_id,
        img.imaging_exam_id,
        fna.fna_episode_id,
        img.exam_date_native  AS img_date,
        fna.fna_date_native   AS fna_date,
        ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native))
                              AS day_gap,
        img.laterality        AS img_laterality,
        fna.fna_lat           AS fna_laterality,
        img.size_cm_max       AS img_size_cm,
        fna.fna_nodule_size_cm AS fna_size_cm,
        {temporal} AS temporal_score,
        {laterality} AS laterality_score,
        {size}        AS size_score
    FROM img
    JOIN fna ON img.research_id = fna.research_id
    WHERE ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native)) <= 365
),
with_counts AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY research_id, nodule_id) AS n_candidates
    FROM candidates
),
scored AS (
    SELECT *,
        ROUND(
            GREATEST(0.0,
                0.50 * temporal_score
                + 0.30 * laterality_score
                + 0.20 * size_score
                - {penalty}
            ), 3)              AS linkage_score,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, nodule_id
            ORDER BY
                (0.50 * temporal_score + 0.30 * laterality_score + 0.20 * size_score)
                    DESC,
                day_gap ASC
        ) AS score_rank
    FROM with_counts
)
SELECT
    research_id,
    nodule_id,
    imaging_exam_id,
    fna_episode_id,
    img_date,
    fna_date,
    day_gap,
    img_laterality,
    fna_laterality,
    img_size_cm,
    fna_size_cm,
    n_candidates,
    temporal_score,
    laterality_score,
    size_score,
    linkage_score,
    score_rank,
    {tier}  AS linkage_confidence_tier,
    CONCAT(
        CASE WHEN day_gap = 0 THEN 'same_day' ELSE CAST(day_gap AS VARCHAR) || 'd_gap' END,
        CASE WHEN img_laterality = fna_laterality THEN '+lat_match'
             WHEN img_laterality IS NULL OR fna_laterality IS NULL THEN '+lat_unknown'
             ELSE '+lat_mismatch' END,
        CASE WHEN n_candidates = 1 THEN '+unique_match'
             ELSE '+' || CAST(n_candidates AS VARCHAR) || '_candidates' END
    )       AS linkage_reason_summary,
    (linkage_score >= 0.50)  AS analysis_eligible_link_flag
FROM scored
"""


def phase_b(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase B: Imaging-FNA Linkage Re-run (Relaxed UNION)")
    results: dict = {}

    has_v1 = table_exists(con, "imaging_nodule_master_v1")
    has_v2 = table_exists(con, "imaging_nodule_long_v2")
    has_fna = table_exists(con, "fna_episode_master_v2")

    if not has_fna:
        print("  SKIP: fna_episode_master_v2 not found")
        return results
    if not has_v1 and not has_v2:
        print("  SKIP: neither imaging_nodule_master_v1 nor imaging_nodule_long_v2 found")
        return results

    v1_count = safe_count(con, "SELECT COUNT(*) FROM imaging_nodule_master_v1") if has_v1 else 0
    v2_count = safe_count(con, "SELECT COUNT(*) FROM imaging_nodule_long_v2") if has_v2 else 0
    print(f"  imaging_nodule_master_v1: {v1_count:,} rows")
    print(f"  imaging_nodule_long_v2:   {v2_count:,} rows")
    results["v1_imaging_rows"] = v1_count
    results["v2_imaging_rows"] = v2_count

    before = safe_count(
        con, "SELECT COUNT(*) FROM imaging_fna_linkage_v3"
    ) if table_exists(con, "imaging_fna_linkage_v3") else 0
    print(f"  imaging_fna_linkage_v3 BEFORE: {before:,} rows")

    helpers = _get_scoring_helpers()
    sql = IMAGING_FNA_RELAXED_SQL_TEMPLATE.format(**helpers)
    safe_exec(con, sql, dry)

    after = safe_count(con, "SELECT COUNT(*) FROM imaging_fna_linkage_v3")
    print(f"  imaging_fna_linkage_v3 AFTER:  {after:,} rows")
    results["linkage_before"] = before
    results["linkage_after"] = after

    if after > 0 and not dry:
        eligible = safe_count(
            con,
            "SELECT COUNT(*) FROM imaging_fna_linkage_v3 "
            "WHERE analysis_eligible_link_flag"
        )
        patients = safe_count(
            con,
            "SELECT COUNT(DISTINCT research_id) FROM imaging_fna_linkage_v3"
        )
        tier_df = con.execute(
            "SELECT linkage_confidence_tier, COUNT(*) AS n "
            "FROM imaging_fna_linkage_v3 GROUP BY 1 ORDER BY 2 DESC"
        ).fetchdf()
        print(f"  Analysis-eligible links: {eligible:,}")
        print(f"  Distinct patients linked: {patients:,}")
        print("\n  Tier distribution:")
        for _, row in tier_df.iterrows():
            print(f"    {row['linkage_confidence_tier']:<20} {int(row['n']):>6}")
        results["eligible_links"] = eligible
        results["patients_linked"] = patients

    con.execute(
        "CREATE OR REPLACE TABLE md_imaging_fna_linkage_v3 "
        "AS SELECT * FROM imaging_fna_linkage_v3"
    ) if not dry and after > 0 else None

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Phase C: RAI dose missingness classification
# ─────────────────────────────────────────────────────────────────────────────

RAI_MISSINGNESS_SQL = [
    "ALTER TABLE rai_treatment_episode_v2 ADD COLUMN IF NOT EXISTS dose_missingness_reason VARCHAR;",
    """
    UPDATE rai_treatment_episode_v2
    SET dose_missingness_reason = CASE
        WHEN dose_mci IS NOT NULL THEN 'dose_available'
        WHEN dose_source IS NOT NULL THEN 'source_present_no_dose_stated'
        WHEN surgery_link_score_v3 IS NULL THEN 'linkage_failed'
        ELSE 'no_source_report_available'
    END
    WHERE dose_missingness_reason IS NULL;
    """,
]

RAI_MISSINGNESS_SUMMARY_SQL = """
CREATE OR REPLACE TABLE vw_rai_dose_missingness_summary AS
SELECT
    dose_missingness_reason         AS reason,
    COUNT(*)                        AS n_episodes,
    COUNT(DISTINCT research_id)     AS n_patients,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct,
    CURRENT_TIMESTAMP               AS audited_at
FROM rai_treatment_episode_v2
GROUP BY dose_missingness_reason
ORDER BY n_episodes DESC
"""


def phase_c(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase C: RAI Dose Missingness Classification")
    results: dict = {}

    if not table_exists(con, "rai_treatment_episode_v2"):
        print("  SKIP: rai_treatment_episode_v2 not found")
        return results

    total = safe_count(con, "SELECT COUNT(*) FROM rai_treatment_episode_v2")
    with_dose = safe_count(
        con, "SELECT COUNT(*) FROM rai_treatment_episode_v2 WHERE dose_mci IS NOT NULL"
    )
    print(f"  RAI episodes: {total:,} total, {with_dose:,} with dose ({100*with_dose/max(total,1):.1f}%)")

    for sql in RAI_MISSINGNESS_SQL:
        safe_exec(con, sql, dry)

    safe_exec(con, RAI_MISSINGNESS_SUMMARY_SQL, dry)

    if not dry:
        summary = con.execute(
            "SELECT * FROM vw_rai_dose_missingness_summary"
        ).fetchdf()
        print("\n  Missingness Breakdown:")
        for _, row in summary.iterrows():
            print(f"    {row['reason']:<35} n={int(row['n_episodes']):>5}  ({row['pct']}%)")
        results["rai_total"] = total
        results["rai_with_dose"] = with_dose
        results["rai_missingness"] = summary.to_dict("records")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Phase D: Lab canonical contract validation
# ─────────────────────────────────────────────────────────────────────────────

VAL_LAB_CANONICAL_SQL = """
CREATE OR REPLACE TABLE val_lab_canonical_v1 AS
WITH plausibility AS (
    SELECT
        lab_name_standardized,
        analyte_group,
        COUNT(*) AS n_total,
        COUNT(*) FILTER (WHERE value_numeric IS NOT NULL) AS n_numeric,
        COUNT(*) FILTER (WHERE value_numeric IS NULL AND value_raw IS NOT NULL) AS n_parse_failures,
        COUNT(*) FILTER (WHERE is_censored IS TRUE) AS n_censored,
        COUNT(*) FILTER (WHERE lab_date > CURRENT_DATE) AS n_future_dates,
        MIN(value_numeric) AS val_min,
        MAX(value_numeric) AS val_max,
        ROUND(AVG(value_numeric), 2) AS val_mean,
        -- Plausibility bounds violations
        COUNT(*) FILTER (WHERE lab_name_standardized = 'thyroglobulin'
            AND value_numeric IS NOT NULL AND (value_numeric < 0 OR value_numeric > 100000)) AS n_tg_oob,
        COUNT(*) FILTER (WHERE lab_name_standardized = 'anti_thyroglobulin'
            AND value_numeric IS NOT NULL AND (value_numeric < 0 OR value_numeric > 10000)) AS n_anti_tg_oob,
        COUNT(*) FILTER (WHERE lab_name_standardized = 'pth'
            AND value_numeric IS NOT NULL AND (value_numeric < 0.5 OR value_numeric > 500)) AS n_pth_oob,
        COUNT(*) FILTER (WHERE lab_name_standardized = 'calcium_total'
            AND value_numeric IS NOT NULL AND (value_numeric < 4 OR value_numeric > 15)) AS n_ca_oob,
        COUNT(*) FILTER (WHERE lab_name_standardized = 'calcium_ionized'
            AND value_numeric IS NOT NULL AND (value_numeric < 0.5 OR value_numeric > 2.0)) AS n_ica_oob
    FROM longitudinal_lab_canonical_v1
    GROUP BY lab_name_standardized, analyte_group
),
duplicates AS (
    SELECT COUNT(*) AS n_exact_dupes
    FROM (
        SELECT research_id, lab_date, lab_name_standardized, value_numeric,
               COUNT(*) AS cnt
        FROM longitudinal_lab_canonical_v1
        WHERE value_numeric IS NOT NULL
        GROUP BY 1, 2, 3, 4
        HAVING COUNT(*) > 1
    ) dups
),
date_issues AS (
    SELECT COUNT(*) AS n_future_dates
    FROM longitudinal_lab_canonical_v1
    WHERE lab_date > CURRENT_DATE
),
tier_check AS (
    SELECT COUNT(*) AS n_invalid_tiers
    FROM longitudinal_lab_canonical_v1
    WHERE data_completeness_tier NOT IN (
        'current_structured', 'current_nlp_partial', 'future_institutional_required'
    )
),
status_check AS (
    SELECT COUNT(*) AS n_invalid_statuses
    FROM longitudinal_lab_canonical_v1
    WHERE lab_date_status NOT IN (
        'exact_collection_date', 'extracted_date', 'unresolved_date'
    ) AND lab_date_status IS NOT NULL
)
SELECT
    p.lab_name_standardized,
    p.analyte_group,
    p.n_total,
    p.n_numeric,
    p.n_parse_failures,
    p.n_censored,
    p.val_min,
    p.val_max,
    p.val_mean,
    (p.n_tg_oob + p.n_anti_tg_oob + p.n_pth_oob + p.n_ca_oob + p.n_ica_oob) AS n_plausibility_violations,
    d.n_exact_dupes,
    di.n_future_dates,
    t.n_invalid_tiers,
    s.n_invalid_statuses,
    CASE
        WHEN p.n_future_dates > 0 THEN 'FAIL'
        WHEN (p.n_tg_oob + p.n_anti_tg_oob + p.n_pth_oob + p.n_ca_oob + p.n_ica_oob) > 0 THEN 'WARN'
        WHEN d.n_exact_dupes > 0 THEN 'WARN'
        WHEN t.n_invalid_tiers > 0 THEN 'FAIL'
        WHEN s.n_invalid_statuses > 0 THEN 'FAIL'
        ELSE 'PASS'
    END AS validation_status,
    CURRENT_TIMESTAMP AS audited_at
FROM plausibility p
CROSS JOIN duplicates d
CROSS JOIN date_issues di
CROSS JOIN tier_check t
CROSS JOIN status_check s
ORDER BY p.n_total DESC
"""


def phase_d(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase D: Lab Canonical Contract Validation")
    results: dict = {}

    if not table_exists(con, "longitudinal_lab_canonical_v1"):
        print("  SKIP: longitudinal_lab_canonical_v1 not found")
        return results

    total = safe_count(con, "SELECT COUNT(*) FROM longitudinal_lab_canonical_v1")
    patients = safe_count(
        con, "SELECT COUNT(DISTINCT research_id) FROM longitudinal_lab_canonical_v1"
    )
    print(f"  Canonical lab table: {total:,} rows, {patients:,} patients")

    safe_exec(con, VAL_LAB_CANONICAL_SQL, dry)

    if not dry:
        val = con.execute("SELECT * FROM val_lab_canonical_v1").fetchdf()
        print("\n  Validation Results:")
        for _, row in val.iterrows():
            status = row["validation_status"]
            marker = "PASS" if status == "PASS" else f"**{status}**"
            print(f"    {row['lab_name_standardized']:<25} "
                  f"n={int(row['n_total']):>6}  "
                  f"plausibility_violations={int(row['n_plausibility_violations'])}  "
                  f"{marker}")
        results["lab_total"] = total
        results["lab_patients"] = patients
        results["lab_validation"] = val.to_dict("records")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Final hardening pass")
    ap.add_argument("--md", action="store_true", help="Use MotherDuck")
    ap.add_argument("--local", action="store_true", help="Use local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Print SQL only")
    ap.add_argument("--phase", default="all",
                    help="Phase to run: A/B/C/D/all")
    args = ap.parse_args()

    con = connect(args)
    dry = args.dry_run
    phases = args.phase.upper().split(",") if args.phase != "all" else ["A", "B", "C", "D"]

    all_results: dict = {}
    if "A" in phases:
        all_results["phase_a"] = phase_a(con, dry)
    if "B" in phases:
        all_results["phase_b"] = phase_b(con, dry)
    if "C" in phases:
        all_results["phase_c"] = phase_c(con, dry)
    if "D" in phases:
        all_results["phase_d"] = phase_d(con, dry)

    out_dir = ROOT / "exports" / f"hardening_audit_{TIMESTAMP}"
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "check_results.json"
    with open(report_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n  Report: {report_path}")

    con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
