#!/usr/bin/env python3
"""
49_enhanced_linkage_v3.py -- Enhanced cross-domain linkage with numeric scoring

Adds richer linkage scoring on top of the existing v2 categorical tiers.
Creates v3 linkage tables that COEXIST with v2 (no v2 modifications).

New scoring framework (0.0–1.0 numeric):
  linkage_score = w_temporal * temporal_score
                + w_laterality * laterality_score
                + w_size * size_score
                + penalty * ambiguity_count

Output columns added vs v2:
  - linkage_score          : numeric 0.0-1.0
  - linkage_confidence_tier: same categorical values as v2 for compatibility
  - linkage_reason_summary : human-readable rationale string
  - analysis_eligible_link_flag: TRUE when score >= 0.5
  - n_candidates           : count of possible matches for this source episode
  - score_rank             : rank among candidates (1 = best)

New tables:
  imaging_fna_linkage_v3
  fna_molecular_linkage_v3
  preop_surgery_linkage_v3
  surgery_pathology_linkage_v3
  pathology_rai_linkage_v3
  linkage_summary_v3
  linkage_ambiguity_review_v1  -- all multi-candidate linkages for review

Run after scripts 22 and 50 (canonical episodes + multinodule).
Supports --md, --local, --dry-run flags.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"

sys.path.insert(0, str(ROOT))

TODAY = datetime.now().strftime("%Y%m%d_%H%M")


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def _get_token() -> str:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if token:
        return token
    secrets = ROOT / ".streamlit" / "secrets.toml"
    if secrets.exists():
        try:
            import toml
            return toml.load(str(secrets))["MOTHERDUCK_TOKEN"]
        except Exception:
            pass
    raise RuntimeError(
        "MOTHERDUCK_TOKEN not set. Export it or add to .streamlit/secrets.toml."
    )


def connect_md() -> duckdb.DuckDBPyConnection:
    token = _get_token()
    return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")


def connect_local() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(DB_PATH))


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Temporal score helper (SQL expression)
# ─────────────────────────────────────────────────────────────────────────────
def temporal_score_sql(gap_expr: str) -> str:
    """
    Returns SQL CASE expression for temporal_score (0.0-1.0).
    gap_expr: SQL expression for absolute day gap (already DOUBLE or INTEGER).
    Rationale:
      - Same day: perfect temporal match
      - <7d: typical same-visit scheduling (imaging -> FNA -> molecular)
      - 8-30d: common short-interval decision window
      - 31-90d: plausible preop planning window
      - 91-365d: extended surveillance window (weak)
      - >365d: no temporal support
    """
    return f"""
        CASE
            WHEN {gap_expr} = 0                          THEN 1.0
            WHEN {gap_expr} <= 7                         THEN 0.9 - 0.01 * {gap_expr}
            WHEN {gap_expr} <= 30                        THEN 0.7 - 0.005 * ({gap_expr} - 7)
            WHEN {gap_expr} <= 90                        THEN 0.5 - 0.003 * ({gap_expr} - 30)
            WHEN {gap_expr} <= 365                       THEN 0.3 - 0.001 * ({gap_expr} - 90)
            ELSE 0.0
        END
    """


def laterality_score_sql(lat1_expr: str, lat2_expr: str) -> str:
    """
    Returns SQL expression for laterality_score (0.0-1.0).
    Isthmus gets partial credit vs any lobe (ambiguous anatomically).
    """
    return f"""
        CASE
            WHEN {lat1_expr} IS NULL OR {lat2_expr} IS NULL      THEN 0.5
            WHEN LOWER({lat1_expr}) = LOWER({lat2_expr})         THEN 1.0
            WHEN LOWER({lat1_expr}) = 'isthmus'
              OR LOWER({lat2_expr}) = 'isthmus'                  THEN 0.3
            ELSE 0.0
        END
    """


def size_compat_score_sql(size1_expr: str, size2_expr: str) -> str:
    """
    Returns SQL expression for size compatibility (0.0-1.0).
    Only meaningful for imaging -> FNA / imaging -> pathology pairs.
    """
    return f"""
        CASE
            WHEN {size1_expr} IS NULL OR {size2_expr} IS NULL    THEN 0.5
            WHEN ABS({size1_expr} - {size2_expr}) <= 0.5         THEN 1.0
            WHEN ABS({size1_expr} - {size2_expr}) <= 1.0         THEN 0.7
            WHEN ABS({size1_expr} - {size2_expr}) <= 2.0         THEN 0.3
            ELSE 0.0
        END
    """


def ambiguity_penalty_sql(n_candidates_expr: str) -> str:
    """Returns SQL expression for ambiguity penalty (0.0-0.2)."""
    return f"""
        CASE
            WHEN {n_candidates_expr} = 1  THEN 0.0
            WHEN {n_candidates_expr} = 2  THEN 0.1
            ELSE 0.2
        END
    """


def confidence_tier_from_score_sql(score_expr: str) -> str:
    """Map numeric score back to categorical tier for v2 compatibility."""
    return f"""
        CASE
            WHEN {score_expr} >= 0.85 THEN 'exact_match'
            WHEN {score_expr} >= 0.65 THEN 'high_confidence'
            WHEN {score_expr} >= 0.45 THEN 'plausible'
            WHEN {score_expr} > 0.0   THEN 'weak'
            ELSE 'unlinked'
        END
    """


# ─────────────────────────────────────────────────────────────────────────────
# A. Imaging nodule -> FNA episode
# ─────────────────────────────────────────────────────────────────────────────
LINK_IMAGING_FNA_V3_SQL = """
CREATE OR REPLACE TABLE imaging_fna_linkage_v3 AS
WITH img AS (
    SELECT research_id, nodule_id, imaging_exam_id,
           exam_date_native, laterality, size_cm_max
    FROM imaging_nodule_long_v2
    WHERE exam_date_native IS NOT NULL

    UNION ALL

    -- Supplement from imaging_nodule_master_v1 if available
    SELECT research_id, nodule_id, exam_id AS imaging_exam_id,
           exam_date, laterality, max_dimension_cm AS size_cm_max
    FROM imaging_nodule_master_v1
    WHERE exam_date IS NOT NULL
      AND research_id NOT IN (
          SELECT DISTINCT research_id FROM imaging_nodule_long_v2
          WHERE exam_date_native IS NOT NULL)
),
fna AS (
    SELECT research_id, fna_episode_id, fna_date_native,
           laterality AS fna_lat, bethesda_category, specimen_site_raw,
           TRY_CAST(nodule_size_cm AS DOUBLE) AS fna_nodule_size_cm
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
        -- Temporal score
        {temporal} AS temporal_score,
        -- Laterality score
        {laterality} AS laterality_score,
        -- Size compatibility score
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
        -- Weighted linkage score (weights: temporal 0.5, laterality 0.3, size 0.2)
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
    -- Categorical tier for backward compatibility
    {tier}  AS linkage_confidence_tier,
    -- Human-readable rationale
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
""".format(
    temporal=temporal_score_sql("ABS(DATEDIFF('day', img.exam_date_native, fna.fna_date_native))"),
    laterality=laterality_score_sql("img.laterality", "fna.fna_lat"),
    size=size_compat_score_sql("img.size_cm_max", "fna.fna_nodule_size_cm"),
    penalty=ambiguity_penalty_sql(
        "COUNT(*) OVER (PARTITION BY research_id, nodule_id)"
    ),
    tier=confidence_tier_from_score_sql("linkage_score"),
)


# ─────────────────────────────────────────────────────────────────────────────
# B. FNA -> Molecular test
# ─────────────────────────────────────────────────────────────────────────────
LINK_FNA_MOLECULAR_V3_SQL = """
CREATE OR REPLACE TABLE fna_molecular_linkage_v3 AS
WITH fna AS (
    SELECT research_id, fna_episode_id, fna_date_native, laterality
    FROM fna_episode_master_v2
    WHERE fna_date_native IS NOT NULL
),
mol AS (
    SELECT research_id, molecular_episode_id, test_date_native, platform
    FROM molecular_test_episode_v2
    WHERE test_date_native IS NOT NULL
),
candidates AS (
    SELECT
        fna.research_id,
        fna.fna_episode_id,
        mol.molecular_episode_id,
        fna.fna_date_native,
        mol.test_date_native,
        -- FNA-before-molecular chronology enforced (-7d tolerance for recording delay)
        DATEDIFF('day', fna.fna_date_native, mol.test_date_native) AS day_gap,
        ABS(DATEDIFF('day', fna.fna_date_native, mol.test_date_native)) AS abs_gap,
        fna.laterality,
        mol.platform
    FROM fna
    JOIN mol ON fna.research_id = mol.research_id
    WHERE DATEDIFF('day', fna.fna_date_native, mol.test_date_native) BETWEEN -7 AND 180
),
with_counts AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY research_id, fna_episode_id) AS n_candidates
    FROM candidates
),
scored AS (
    SELECT *,
        ROUND(
            GREATEST(0.0,
                0.70 * {temporal}
                + 0.30 * 0.5  -- laterality not applicable for mol tests; give neutral 0.5
                - {penalty}
            ), 3)              AS linkage_score,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, fna_episode_id
            ORDER BY abs_gap ASC, day_gap ASC
        ) AS score_rank
    FROM with_counts
)
SELECT
    research_id,
    fna_episode_id,
    molecular_episode_id,
    fna_date_native,
    test_date_native,
    day_gap,
    abs_gap,
    laterality,
    platform,
    n_candidates,
    linkage_score,
    score_rank,
    {tier}  AS linkage_confidence_tier,
    CONCAT(
        CASE WHEN abs_gap = 0 THEN 'same_day'
             WHEN day_gap < 0 THEN CAST(ABS(day_gap) AS VARCHAR) || 'd_before_fna'
             ELSE CAST(day_gap AS VARCHAR) || 'd_after_fna' END,
        '+', COALESCE(platform, 'unknown_platform'),
        CASE WHEN n_candidates = 1 THEN '+unique' ELSE '+multi_candidate' END
    ) AS linkage_reason_summary,
    (linkage_score >= 0.50) AS analysis_eligible_link_flag
FROM scored
""".format(
    temporal=temporal_score_sql("abs_gap"),
    penalty=ambiguity_penalty_sql("COUNT(*) OVER (PARTITION BY research_id, fna_episode_id)"),
    tier=confidence_tier_from_score_sql("linkage_score"),
)


# ─────────────────────────────────────────────────────────────────────────────
# C. FNA/Molecular -> Surgery
# ─────────────────────────────────────────────────────────────────────────────
LINK_PREOP_SURGERY_V3_SQL = """
CREATE OR REPLACE TABLE preop_surgery_linkage_v3 AS
WITH preop AS (
    SELECT research_id, fna_episode_id AS preop_episode_id, 'fna' AS preop_type,
           fna_date_native AS preop_date, laterality
    FROM fna_episode_master_v2
    WHERE fna_date_native IS NOT NULL
    UNION ALL
    SELECT research_id, molecular_episode_id, 'molecular',
           test_date_native, NULL
    FROM molecular_test_episode_v2
    WHERE test_date_native IS NOT NULL
),
surg AS (
    SELECT research_id, surgery_episode_id, surgery_date_native,
           laterality AS surg_lat
    FROM operative_episode_detail_v2
    WHERE surgery_date_native IS NOT NULL
),
candidates AS (
    SELECT
        p.research_id,
        p.preop_episode_id,
        p.preop_type,
        s.surgery_episode_id,
        p.preop_date,
        s.surgery_date_native  AS surgery_date,
        DATEDIFF('day', p.preop_date, s.surgery_date_native) AS day_gap,
        ABS(DATEDIFF('day', p.preop_date, s.surgery_date_native)) AS abs_gap,
        p.laterality           AS preop_lat,
        s.surg_lat
    FROM preop p
    JOIN surg s ON p.research_id = s.research_id
    -- Must be preop-before-surgery (with -7d recording tolerance)
    WHERE DATEDIFF('day', p.preop_date, s.surgery_date_native) BETWEEN -7 AND 365
),
with_counts AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY research_id, preop_episode_id) AS n_candidates
    FROM candidates
),
scored AS (
    SELECT *,
        ROUND(
            GREATEST(0.0,
                0.60 * {temporal}
                + 0.40 * {laterality}
                - {penalty}
            ), 3) AS linkage_score,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, preop_episode_id
            ORDER BY abs_gap ASC
        ) AS score_rank
    FROM with_counts
)
SELECT
    research_id,
    preop_episode_id,
    preop_type,
    surgery_episode_id,
    preop_date,
    surgery_date,
    day_gap,
    abs_gap,
    preop_lat,
    surg_lat,
    n_candidates,
    linkage_score,
    score_rank,
    {tier} AS linkage_confidence_tier,
    CONCAT(
        CAST(day_gap AS VARCHAR) || 'd_to_surgery',
        CASE WHEN preop_lat IS NOT NULL AND surg_lat IS NOT NULL
                  AND preop_lat = surg_lat THEN '+lat_match'
             WHEN preop_lat IS NULL OR surg_lat IS NULL THEN '+lat_unknown'
             ELSE '+lat_mismatch' END,
        '+', preop_type
    ) AS linkage_reason_summary,
    (linkage_score >= 0.50) AS analysis_eligible_link_flag
FROM scored
""".format(
    temporal=temporal_score_sql("abs_gap"),
    laterality=laterality_score_sql("p.laterality", "s.surg_lat"),
    penalty=ambiguity_penalty_sql("COUNT(*) OVER (PARTITION BY research_id, preop_episode_id)"),
    tier=confidence_tier_from_score_sql("linkage_score"),
)


# ─────────────────────────────────────────────────────────────────────────────
# D. Surgery -> Pathology tumor
# ─────────────────────────────────────────────────────────────────────────────
LINK_SURGERY_PATHOLOGY_V3_SQL = """
CREATE OR REPLACE TABLE surgery_pathology_linkage_v3 AS
WITH surg AS (
    SELECT research_id, surgery_episode_id, surgery_date_native,
           laterality AS surg_lat
    FROM operative_episode_detail_v2
),
path AS (
    SELECT research_id, surgery_episode_id AS path_surgery_id,
           tumor_ordinal, surgery_date AS path_date,
           laterality AS path_lat,
           tumor_size_cm AS path_size_cm
    FROM tumor_episode_master_v2
),
candidates AS (
    SELECT
        s.research_id,
        s.surgery_episode_id,
        p.path_surgery_id,
        p.tumor_ordinal,
        s.surgery_date_native  AS surg_date,
        p.path_date,
        ABS(DATEDIFF('day', COALESCE(s.surgery_date_native, '1900-01-01'),
                             COALESCE(TRY_CAST(p.path_date AS DATE), '1900-01-01')))
                               AS day_gap,
        s.surg_lat,
        p.path_lat,
        p.path_size_cm
    FROM surg s
    JOIN path p ON s.research_id = p.research_id
),
with_counts AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY research_id, surgery_episode_id) AS n_candidates
    FROM candidates
),
scored AS (
    SELECT *,
        ROUND(
            GREATEST(0.0,
                0.55 * {temporal}
                + 0.45 * {laterality}
                - {penalty}
            ), 3) AS linkage_score,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, surgery_episode_id
            ORDER BY tumor_ordinal ASC, day_gap ASC
        ) AS score_rank
    FROM with_counts
)
SELECT
    research_id,
    surgery_episode_id,
    path_surgery_id,
    tumor_ordinal,
    surg_date,
    path_date,
    day_gap,
    surg_lat,
    path_lat,
    path_size_cm,
    n_candidates,
    linkage_score,
    score_rank,
    {tier} AS linkage_confidence_tier,
    CONCAT(
        CASE WHEN day_gap = 0 THEN 'same_day' ELSE CAST(day_gap AS VARCHAR) || 'd_gap' END,
        CASE WHEN surg_lat IS NOT NULL AND path_lat IS NOT NULL
                  AND surg_lat = path_lat THEN '+lat_match'
             WHEN surg_lat IS NULL OR path_lat IS NULL THEN '+lat_unknown'
             ELSE '+lat_mismatch' END,
        '+tumor_ordinal_' || CAST(tumor_ordinal AS VARCHAR)
    ) AS linkage_reason_summary,
    (linkage_score >= 0.50) AS analysis_eligible_link_flag
FROM scored
""".format(
    temporal=temporal_score_sql("day_gap"),
    laterality=laterality_score_sql("s.surg_lat", "p.path_lat"),
    penalty=ambiguity_penalty_sql("COUNT(*) OVER (PARTITION BY research_id, surgery_episode_id)"),
    tier=confidence_tier_from_score_sql("linkage_score"),
)


# ─────────────────────────────────────────────────────────────────────────────
# E. Pathology -> RAI treatment
# ─────────────────────────────────────────────────────────────────────────────
LINK_PATHOLOGY_RAI_V3_SQL = """
CREATE OR REPLACE TABLE pathology_rai_linkage_v3 AS
WITH path AS (
    SELECT research_id, surgery_episode_id,
           TRY_CAST(surgery_date AS DATE) AS surgery_date
    FROM tumor_episode_master_v2
    QUALIFY ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY surgery_date ASC NULLS LAST) = 1
),
rai AS (
    SELECT research_id, rai_episode_id, resolved_rai_date AS rai_date,
           rai_assertion_status, dose_mci
    FROM rai_treatment_episode_v2
    WHERE rai_assertion_status IN ('definite_received','likely_received')
),
candidates AS (
    SELECT
        p.research_id,
        p.surgery_episode_id,
        r.rai_episode_id,
        p.surgery_date,
        r.rai_date,
        DATEDIFF('day', p.surgery_date, TRY_CAST(r.rai_date AS DATE)) AS days_post_surgery,
        ABS(DATEDIFF('day', p.surgery_date, TRY_CAST(r.rai_date AS DATE))) AS abs_days,
        r.rai_assertion_status,
        r.dose_mci
    FROM path p
    JOIN rai r ON p.research_id = r.research_id
    -- RAI expected 14-365d post-surgery; allow -14d for early planning documentation
    WHERE DATEDIFF('day', p.surgery_date, TRY_CAST(r.rai_date AS DATE)) BETWEEN -14 AND 365
),
with_counts AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY research_id, surgery_episode_id) AS n_candidates
    FROM candidates
),
scored AS (
    SELECT *,
        -- For RAI: temporal window is 2-12 weeks post-surgery (ideal)
        ROUND(
            GREATEST(0.0,
                0.70 * CASE
                    WHEN days_post_surgery BETWEEN 14 AND 90  THEN 1.0
                    WHEN days_post_surgery BETWEEN 1 AND 180  THEN 0.8
                    WHEN days_post_surgery BETWEEN 0 AND 365  THEN 0.5
                    WHEN days_post_surgery < 0                THEN 0.2
                    ELSE 0.1 END
                + 0.20 * CASE
                    WHEN rai_assertion_status = 'definite_received' THEN 1.0
                    WHEN rai_assertion_status = 'likely_received'   THEN 0.8
                    ELSE 0.5 END
                + 0.10 * CASE WHEN dose_mci IS NOT NULL THEN 1.0 ELSE 0.0 END
                - {penalty}
            ), 3) AS linkage_score,
        ROW_NUMBER() OVER (
            PARTITION BY research_id, surgery_episode_id
            ORDER BY days_post_surgery ASC
        ) AS score_rank
    FROM with_counts
)
SELECT
    research_id,
    surgery_episode_id,
    rai_episode_id,
    surgery_date,
    rai_date,
    days_post_surgery,
    abs_days,
    rai_assertion_status,
    dose_mci,
    n_candidates,
    linkage_score,
    score_rank,
    {tier} AS linkage_confidence_tier,
    CONCAT(
        CAST(days_post_surgery AS VARCHAR) || 'd_post_surgery',
        '+', COALESCE(rai_assertion_status, 'unknown_status'),
        CASE WHEN dose_mci IS NOT NULL
             THEN '+' || CAST(ROUND(dose_mci, 0) AS VARCHAR) || 'mCi'
             ELSE '' END
    ) AS linkage_reason_summary,
    (linkage_score >= 0.50) AS analysis_eligible_link_flag
FROM scored
""".format(
    penalty=ambiguity_penalty_sql("COUNT(*) OVER (PARTITION BY research_id, surgery_episode_id)"),
    tier=confidence_tier_from_score_sql("linkage_score"),
)


# ─────────────────────────────────────────────────────────────────────────────
# Linkage summary v3
# ─────────────────────────────────────────────────────────────────────────────
LINKAGE_SUMMARY_V3_SQL = """
CREATE OR REPLACE TABLE linkage_summary_v3 AS
SELECT
    'imaging_fna' AS linkage_type,
    COUNT(*) AS total_links,
    SUM(CASE WHEN analysis_eligible_link_flag THEN 1 ELSE 0 END) AS eligible_links,
    ROUND(AVG(linkage_score), 3) AS mean_score,
    ROUND(MIN(linkage_score), 3) AS min_score,
    ROUND(MAX(linkage_score), 3) AS max_score,
    SUM(CASE WHEN linkage_confidence_tier = 'exact_match'    THEN 1 ELSE 0 END) AS n_exact,
    SUM(CASE WHEN linkage_confidence_tier = 'high_confidence' THEN 1 ELSE 0 END) AS n_high,
    SUM(CASE WHEN linkage_confidence_tier = 'plausible'       THEN 1 ELSE 0 END) AS n_plausible,
    SUM(CASE WHEN linkage_confidence_tier = 'weak'            THEN 1 ELSE 0 END) AS n_weak,
    SUM(CASE WHEN n_candidates > 1     THEN 1 ELSE 0 END) AS n_ambiguous,
    CURRENT_TIMESTAMP AS created_at
FROM imaging_fna_linkage_v3

UNION ALL

SELECT 'fna_molecular', COUNT(*),
    SUM(CASE WHEN analysis_eligible_link_flag THEN 1 ELSE 0 END),
    ROUND(AVG(linkage_score), 3), ROUND(MIN(linkage_score), 3),
    ROUND(MAX(linkage_score), 3),
    SUM(CASE WHEN linkage_confidence_tier='exact_match' THEN 1 ELSE 0 END),
    SUM(CASE WHEN linkage_confidence_tier='high_confidence' THEN 1 ELSE 0 END),
    SUM(CASE WHEN linkage_confidence_tier='plausible' THEN 1 ELSE 0 END),
    SUM(CASE WHEN linkage_confidence_tier='weak' THEN 1 ELSE 0 END),
    SUM(CASE WHEN n_candidates>1 THEN 1 ELSE 0 END),
    CURRENT_TIMESTAMP
FROM fna_molecular_linkage_v3

UNION ALL

SELECT 'preop_surgery', COUNT(*),
    SUM(CASE WHEN analysis_eligible_link_flag THEN 1 ELSE 0 END),
    ROUND(AVG(linkage_score), 3), ROUND(MIN(linkage_score), 3),
    ROUND(MAX(linkage_score), 3),
    SUM(CASE WHEN linkage_confidence_tier='exact_match' THEN 1 ELSE 0 END),
    SUM(CASE WHEN linkage_confidence_tier='high_confidence' THEN 1 ELSE 0 END),
    SUM(CASE WHEN linkage_confidence_tier='plausible' THEN 1 ELSE 0 END),
    SUM(CASE WHEN linkage_confidence_tier='weak' THEN 1 ELSE 0 END),
    SUM(CASE WHEN n_candidates>1 THEN 1 ELSE 0 END),
    CURRENT_TIMESTAMP
FROM preop_surgery_linkage_v3

UNION ALL

SELECT 'surgery_pathology', COUNT(*),
    SUM(CASE WHEN analysis_eligible_link_flag THEN 1 ELSE 0 END),
    ROUND(AVG(linkage_score), 3), ROUND(MIN(linkage_score), 3),
    ROUND(MAX(linkage_score), 3),
    SUM(CASE WHEN linkage_confidence_tier='exact_match' THEN 1 ELSE 0 END),
    SUM(CASE WHEN linkage_confidence_tier='high_confidence' THEN 1 ELSE 0 END),
    SUM(CASE WHEN linkage_confidence_tier='plausible' THEN 1 ELSE 0 END),
    SUM(CASE WHEN linkage_confidence_tier='weak' THEN 1 ELSE 0 END),
    SUM(CASE WHEN n_candidates>1 THEN 1 ELSE 0 END),
    CURRENT_TIMESTAMP
FROM surgery_pathology_linkage_v3

UNION ALL

SELECT 'pathology_rai', COUNT(*),
    SUM(CASE WHEN analysis_eligible_link_flag THEN 1 ELSE 0 END),
    ROUND(AVG(linkage_score), 3), ROUND(MIN(linkage_score), 3),
    ROUND(MAX(linkage_score), 3),
    SUM(CASE WHEN linkage_confidence_tier='exact_match' THEN 1 ELSE 0 END),
    SUM(CASE WHEN linkage_confidence_tier='high_confidence' THEN 1 ELSE 0 END),
    SUM(CASE WHEN linkage_confidence_tier='plausible' THEN 1 ELSE 0 END),
    SUM(CASE WHEN linkage_confidence_tier='weak' THEN 1 ELSE 0 END),
    SUM(CASE WHEN n_candidates>1 THEN 1 ELSE 0 END),
    CURRENT_TIMESTAMP
FROM pathology_rai_linkage_v3
"""

# ─────────────────────────────────────────────────────────────────────────────
# Ambiguity review table
# ─────────────────────────────────────────────────────────────────────────────
AMBIGUITY_REVIEW_SQL = """
CREATE OR REPLACE TABLE linkage_ambiguity_review_v1 AS

-- Imaging -> FNA multi-candidate cases
SELECT
    research_id,
    'imaging_fna'               AS linkage_type,
    CAST(nodule_id AS VARCHAR)  AS source_episode_id,
    CAST(fna_episode_id AS VARCHAR) AS target_episode_id,
    linkage_score,
    score_rank,
    n_candidates,
    linkage_confidence_tier,
    linkage_reason_summary,
    'Imaging nodule has multiple FNA candidates; review which was biopsied'
                                AS review_instruction
FROM imaging_fna_linkage_v3
WHERE n_candidates > 1

UNION ALL

SELECT
    research_id,
    'fna_molecular',
    CAST(fna_episode_id AS VARCHAR),
    CAST(molecular_episode_id AS VARCHAR),
    linkage_score, score_rank, n_candidates,
    linkage_confidence_tier, linkage_reason_summary,
    'FNA episode linked to multiple molecular tests; verify correct specimen'
FROM fna_molecular_linkage_v3
WHERE n_candidates > 1

UNION ALL

SELECT
    research_id,
    'preop_surgery',
    preop_episode_id,
    CAST(surgery_episode_id AS VARCHAR),
    linkage_score, score_rank, n_candidates,
    linkage_confidence_tier, linkage_reason_summary,
    'Preop episode linked to multiple surgeries; verify correct procedure'
FROM preop_surgery_linkage_v3
WHERE n_candidates > 1

UNION ALL

SELECT
    research_id,
    'surgery_pathology',
    CAST(surgery_episode_id AS VARCHAR),
    CAST(path_surgery_id AS VARCHAR),
    linkage_score, score_rank, n_candidates,
    linkage_confidence_tier, linkage_reason_summary,
    'Surgery linked to multiple pathology reports; verify specimen identity'
FROM surgery_pathology_linkage_v3
WHERE n_candidates > 1

UNION ALL

SELECT
    research_id,
    'pathology_rai',
    CAST(surgery_episode_id AS VARCHAR),
    CAST(rai_episode_id AS VARCHAR),
    linkage_score, score_rank, n_candidates,
    linkage_confidence_tier, linkage_reason_summary,
    'Pathology episode linked to multiple RAI treatments; verify treatment course'
FROM pathology_rai_linkage_v3
WHERE n_candidates > 1
"""


LINKAGE_TASKS = [
    ("imaging_fna_linkage_v3",         LINK_IMAGING_FNA_V3_SQL,      "imaging_nodule_long_v2"),
    ("fna_molecular_linkage_v3",       LINK_FNA_MOLECULAR_V3_SQL,    "fna_episode_master_v2"),
    ("preop_surgery_linkage_v3",       LINK_PREOP_SURGERY_V3_SQL,    "fna_episode_master_v2"),
    ("surgery_pathology_linkage_v3",   LINK_SURGERY_PATHOLOGY_V3_SQL,"operative_episode_detail_v2"),
    ("pathology_rai_linkage_v3",       LINK_PATHOLOGY_RAI_V3_SQL,    "tumor_episode_master_v2"),
]


def build_linkage_tables(con: duckdb.DuckDBPyConnection,
                         dry_run: bool = False) -> None:
    section("Building enhanced linkage v3 tables")

    if not table_available(con, "fna_episode_master_v2"):
        print("  [SKIP] fna_episode_master_v2 not found -- run script 22 first")
        return

    # imaging_nodule_long_v2 may be empty; supplement with imaging_nodule_master_v1
    if not table_available(con, "imaging_nodule_long_v2"):
        print("  [WARN] imaging_nodule_long_v2 not found; imaging-FNA linkage "
              "will use imaging_nodule_master_v1 only")
        # Create a minimal stub so the SQL UNION ALL doesn't fail
        con.execute("""
CREATE OR REPLACE TEMP TABLE imaging_nodule_long_v2 AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS nodule_id,
       NULL::VARCHAR AS imaging_exam_id, NULL::DATE AS exam_date_native,
       NULL::VARCHAR AS laterality, NULL::DOUBLE AS size_cm_max
WHERE 1=0
""")

    # imaging_nodule_master_v1 may not exist yet
    if not table_available(con, "imaging_nodule_master_v1"):
        con.execute("""
CREATE OR REPLACE TEMP TABLE imaging_nodule_master_v1 AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS nodule_id,
       NULL::VARCHAR AS exam_id, NULL::DATE AS exam_date,
       NULL::VARCHAR AS laterality, NULL::DOUBLE AS max_dimension_cm
WHERE 1=0
""")

    if not table_available(con, "rai_treatment_episode_v2"):
        con.execute("""
CREATE OR REPLACE TEMP TABLE rai_treatment_episode_v2 AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS rai_episode_id,
       NULL::DATE AS resolved_rai_date, NULL::VARCHAR AS rai_assertion_status,
       NULL::DOUBLE AS dose_mci
WHERE 1=0
""")

    if dry_run:
        print("  [DRY-RUN] Would create: " +
              ", ".join(t[0] for t in LINKAGE_TASKS) +
              ", linkage_summary_v3, linkage_ambiguity_review_v1")
        return

    for tbl_name, sql, dep_tbl in LINKAGE_TASKS:
        if not table_available(con, dep_tbl):
            print(f"  [SKIP] {tbl_name} -- dependency {dep_tbl} not available")
            continue
        print(f"  Building {tbl_name}...")
        try:
            con.execute(sql)
            r = con.execute(f"SELECT COUNT(*) FROM {tbl_name}").fetchone()
            print(f"    {tbl_name}: {r[0]:,} rows")
        except Exception as exc:
            print(f"    [ERROR] {tbl_name}: {exc}")

    print("  Building linkage_summary_v3...")
    try:
        con.execute(LINKAGE_SUMMARY_V3_SQL)
        summary = con.execute("SELECT * FROM linkage_summary_v3").fetchdf()
        print(summary.to_string(index=False))
    except Exception as exc:
        print(f"  [ERROR] linkage_summary_v3: {exc}")

    print("  Building linkage_ambiguity_review_v1...")
    try:
        con.execute(AMBIGUITY_REVIEW_SQL)
        r = con.execute(
            "SELECT linkage_type, COUNT(*) AS n_ambiguous "
            "FROM linkage_ambiguity_review_v1 "
            "GROUP BY linkage_type ORDER BY linkage_type"
        ).fetchdf()
        print(r.to_string(index=False))
    except Exception as exc:
        print(f"  [ERROR] linkage_ambiguity_review_v1: {exc}")

    print("\n  [DONE] Enhanced linkage v3 tables created")


def main() -> None:
    p = argparse.ArgumentParser(description="49_enhanced_linkage_v3.py")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--md", action="store_true", help="Connect to MotherDuck")
    g.add_argument("--local", action="store_true", help="Use local DuckDB (default)")
    p.add_argument("--dry-run", action="store_true", help="Audit only, no writes")
    args = p.parse_args()

    if args.md:
        section("Connecting to MotherDuck")
        con = connect_md()
    else:
        section("Connecting to local DuckDB")
        con = connect_local()

    try:
        build_linkage_tables(con, dry_run=args.dry_run)
    finally:
        con.close()

    print("\n[COMPLETE] 49_enhanced_linkage_v3.py finished")


if __name__ == "__main__":
    main()
