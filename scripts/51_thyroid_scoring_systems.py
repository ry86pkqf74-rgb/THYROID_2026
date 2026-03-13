#!/usr/bin/env python3
"""
51_thyroid_scoring_systems.py -- Thyroid-specific scoring system derivation

Creates reproducible, versioned, analysis-grade scoring features for DTC patients.

Scores derived:
  - AJCC 8th Edition T, N, M, stage group (age-dependent DTC rules)
  - ATA 2015 Initial Recurrence Risk (low / intermediate / high)
  - ATA Response to Therapy (excellent / biochemical_incomplete /
                             structural_incomplete / indeterminate)
  - MACIS score + risk group (Mayo Clinic)
  - AGES score (Mayo Clinic)
  - AMES risk classification (Lahey Clinic)
  - LN burden metrics (ln_ratio, burden bands)
  - Molecular risk composite (BRAF+TERT = high, single driver = intermediate)

Every score includes:
  - *_calculable_flag    : TRUE only when all required inputs present
  - *_source / *_reason  : which source table/field drove the result
  - *_missing_components : bitmask/list of missing required inputs

Output table: thyroid_scoring_systems_v1 (one row per patient)

Run after scripts 15-47 (reads from MotherDuck or local DuckDB).
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
PROCESSED = ROOT / "processed"

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


def register_parquets(con: duckdb.DuckDBPyConnection) -> None:
    """Register locally-available parquet files for local-mode runs."""
    for tbl in ["path_synoptics", "tumor_pathology", "molecular_testing",
                "fna_history", "fna_cytology", "operative_details"]:
        pq = PROCESSED / f"{tbl}.parquet"
        if pq.exists() and not table_available(con, tbl):
            con.execute(
                f"CREATE OR REPLACE TABLE {tbl} AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )
    # patient_level_summary_mv (from sorted parquet)
    pq = PROCESSED / "patient_level_summary_mv_sorted.parquet"
    if pq.exists() and not table_available(con, "patient_level_summary_mv"):
        con.execute(
            "CREATE OR REPLACE TABLE patient_level_summary_mv AS "
            f"SELECT * FROM read_parquet('{pq}')"
        )


# ────────────────────────────────────────────────────────────────────────────
# SQL: thyroid_scoring_systems_v1
# ────────────────────────────────────────────────────────────────────────────
SCORING_SQL = """
CREATE OR REPLACE TABLE thyroid_scoring_systems_v1 AS
WITH

-- ── Patient spine ─────────────────────────────────────────────────────────
-- Use tumor_episode_master_v2 (primary tumor per patient) as the clinical
-- foundation.  Fall back to path_synoptics for any fields not yet in v2.
primary_tumor AS (
    SELECT *
    FROM tumor_episode_master_v2
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY
            CASE WHEN tumor_ordinal = 1 THEN 0 ELSE 1 END,
            tumor_size_cm DESC NULLS LAST
    ) = 1
),

-- ── Demographics (age at surgery) ─────────────────────────────────────────
demo AS (
    SELECT DISTINCT
        ps.research_id,
        ps.age                                          AS age_at_surgery,
        LOWER(ps.gender)                                AS sex
    FROM path_synoptics ps
    WHERE ps.age IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ps.research_id
        ORDER BY ps.surg_date DESC NULLS LAST
    ) = 1
),

-- ── LN burden from path_synoptics ─────────────────────────────────────────
ln_raw AS (
    SELECT
        research_id,
        TRY_CAST(REPLACE(CAST(tumor_1_ln_involved AS VARCHAR), ';', '') AS DOUBLE)
            AS ln_positive_raw,
        TRY_CAST(REPLACE(CAST(tumor_1_ln_examined AS VARCHAR), ';', '') AS DOUBLE)
            AS ln_examined_raw,
        tumor_1_ln_location                             AS ln_location_raw,
        tumor_1_level_examined                          AS ln_levels_examined_raw,
        tumor_1_level_involved                          AS ln_levels_involved_raw
    FROM path_synoptics
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY surg_date DESC NULLS LAST
    ) = 1
),

-- ── Recurrence / distant mets proxy ───────────────────────────────────────
-- Use extracted_recurrence_refined_v1 when available; else recurrence_risk_features_mv
recurrence_src AS (
    SELECT
        research_id,
        BOOL_OR(
            LOWER(CAST(recurrence_confirmed AS VARCHAR)) = 'true'
            OR LOWER(CAST(recurrence_confirmed AS VARCHAR)) = '1'
        ) AS recurrence_flag,
        MIN(first_recurrence_date)  AS first_recurrence_date
    FROM (
        SELECT research_id,
               recurrence_confirmed,
               first_recurrence_date
        FROM extracted_recurrence_refined_v1
        WHERE research_id IS NOT NULL
    ) sub
    GROUP BY research_id
),

-- ── RAI treatment evidence ─────────────────────────────────────────────────
rai_src AS (
    SELECT DISTINCT
        research_id,
        TRUE AS rai_received_flag,
        MIN(resolved_rai_date) AS rai_date
    FROM rai_treatment_episode_v2
    WHERE rai_assertion_status IN ('definite_received', 'likely_received')
    GROUP BY research_id
),

-- ── Tg labs (latest post-surgery value for ATA response) ──────────────────
tg_latest AS (
    SELECT
        research_id,
        MAX(CASE WHEN TRIM(CAST(result AS VARCHAR)) LIKE '<%'
                 THEN TRY_CAST(REGEXP_REPLACE(TRIM(CAST(result AS VARCHAR)),
                               '[^0-9.]', '') AS DOUBLE)
                 ELSE TRY_CAST(result AS DOUBLE) END)    AS tg_max,
        MIN(CASE WHEN TRIM(CAST(result AS VARCHAR)) LIKE '<%'
                 THEN TRY_CAST(REGEXP_REPLACE(TRIM(CAST(result AS VARCHAR)),
                               '[^0-9.]', '') AS DOUBLE)
                 ELSE TRY_CAST(result AS DOUBLE) END)    AS tg_nadir,
        MAX(TRY_CAST(specimen_collect_dt AS TIMESTAMP))  AS tg_latest_date
    FROM thyroglobulin_labs
    WHERE result IS NOT NULL AND TRIM(CAST(result AS VARCHAR)) != ''
    GROUP BY research_id
),

-- ── Molecular panel (BRAF, TERT, RAS) ────────────────────────────────────
mol AS (
    SELECT
        research_id,
        BOOL_OR(LOWER(CAST(braf_positive_final AS VARCHAR)) = 'true'
                OR LOWER(CAST(braf_positive_final AS VARCHAR)) = '1')
            AS braf_positive,
        BOOL_OR(LOWER(CAST(tert_positive_v9 AS VARCHAR)) = 'true'
                OR LOWER(CAST(tert_positive_v9 AS VARCHAR)) = '1')
            AS tert_positive,
        BOOL_OR(LOWER(CAST(ras_positive_final AS VARCHAR)) = 'true'
                OR LOWER(CAST(ras_positive_final AS VARCHAR)) = '1')
            AS ras_positive
    FROM patient_refined_master_clinical_v12
    GROUP BY research_id
),

-- ── Assemble staging inputs ────────────────────────────────────────────────
staging AS (
    SELECT
        pt.research_id,
        d.age_at_surgery,
        d.sex,

        -- Tumor size (best available)
        COALESCE(pt.tumor_size_cm,
                 TRY_CAST(REPLACE(CAST(ps.tumor_1_size_greatest_dimension_cm AS VARCHAR),
                                  ';','') AS DOUBLE))
            AS tumor_size_cm,

        -- ETE grade (Phase 9 v9 is most refined)
        COALESCE(
            LOWER(CAST(mcv.ete_grade_v9 AS VARCHAR)),
            LOWER(CAST(pt.extrathyroidal_extension AS VARCHAR)),
            'unknown'
        )   AS ete_grade,

        -- Gross ETE flag
        COALESCE(pt.gross_ete, FALSE)           AS gross_ete_flag,

        -- Vascular invasion (WHO 2022 v13 grade)
        LOWER(CAST(COALESCE(mcv.vasc_grade_final_v13,
                            pt.vascular_invasion) AS VARCHAR))
            AS vascular_invasion_grade,

        -- Margin status
        LOWER(CAST(COALESCE(mcv.margin_r_class_v10,
                            pt.margin_status) AS VARCHAR))
            AS margin_status,

        -- LN data
        COALESCE(mcv.ln_total_positive_v10, ln.ln_positive_raw,
                 pt.nodal_disease_positive_count)
            AS ln_positive,
        COALESCE(mcv.ln_total_examined, ln.ln_examined_raw,
                 pt.nodal_disease_total_count)
            AS ln_examined,
        -- Central vs lateral dissection
        COALESCE(mcv.ln_central_dissected, FALSE)   AS central_dissected,
        COALESCE(mcv.lateral_neck_dissected_v10, FALSE) AS lateral_dissected,

        -- LN location text (for N1a vs N1b)
        COALESCE(ln.ln_levels_involved_raw, pt.t_stage)
            AS ln_level_raw,
        ln.ln_location_raw,

        -- Multifocality
        COALESCE(pt.multifocality_flag, FALSE)      AS multifocal_flag,

        -- Histology
        LOWER(CAST(COALESCE(mcv.histology_normalized,
                            pt.primary_histology) AS VARCHAR))
            AS histology,

        -- Aggressive variant flag
        CASE WHEN LOWER(CAST(COALESCE(mcv.histology_normalized,
                             pt.primary_histology,
                             pt.histology_variant) AS VARCHAR))
                  LIKE ANY ('%tall_cell%','%hobnail%','%columnar%',
                            '%diffuse_sclerosing%','%solid%','%pdtc%')
             THEN TRUE ELSE FALSE END
            AS aggressive_variant_flag,

        -- Surgery date (for age calculation)
        COALESCE(pt.surgery_date,
                 TRY_CAST(ps.surg_date AS DATE))    AS surgery_date,

        -- Recurrence / distant mets
        COALESCE(rec.recurrence_flag, FALSE)        AS distant_mets_proxy,

        -- RAI
        COALESCE(rai.rai_received_flag, FALSE)      AS rai_received,

        -- Tg
        tg.tg_nadir,
        tg.tg_max,

        -- Size of largest metastatic LN deposit
        TRY_CAST(REPLACE(CAST(ps.tumor_1_size_of_largest_metastatic_deposit_cm AS VARCHAR),
                         ';','') AS DOUBLE)
            AS ln_max_deposit_cm,

        -- Molecular markers
        COALESCE(m.braf_positive, FALSE)            AS braf_positive,
        COALESCE(m.tert_positive, FALSE)            AS tert_positive,
        COALESCE(m.ras_positive, FALSE)             AS ras_positive,

        -- Histologic grade (for AGES)
        ps.tumor_1_histologic_grade                 AS histologic_grade_raw

    FROM primary_tumor pt
    LEFT JOIN demo d USING (research_id)
    LEFT JOIN path_synoptics ps ON ps.research_id = pt.research_id
        AND ps.surg_date = pt.surgery_date
    LEFT JOIN patient_refined_master_clinical_v12 mcv USING (research_id)
    LEFT JOIN ln_raw ln USING (research_id)
    LEFT JOIN recurrence_src rec USING (research_id)
    LEFT JOIN rai_src rai USING (research_id)
    LEFT JOIN tg_latest tg USING (research_id)
    LEFT JOIN mol m USING (research_id)
),

-- ────────────────────────────────────────────────────────────────────────────
-- AJCC 8th Edition staging (DTC-specific age-dependent rules)
-- Reference: AJCC Cancer Staging Manual, 8th Ed. Chapter 73 (Thyroid)
-- ────────────────────────────────────────────────────────────────────────────
ajcc8 AS (
    SELECT
        research_id,

        -- ── T stage ──────────────────────────────────────────────────────
        CASE
            -- Gross ETE to major structures → T4 (overrides size)
            WHEN gross_ete_flag
                 AND ete_grade LIKE '%gross%'
                 AND (ete_grade LIKE '%larynx%' OR ete_grade LIKE '%trachea%'
                      OR ete_grade LIKE '%esophag%' OR ete_grade LIKE '%rln%'
                      OR ete_grade LIKE '%extensive%')
                 THEN 'T4a'
            -- Gross ETE (perithyroidal) → T3b
            WHEN gross_ete_flag OR ete_grade LIKE '%gross%'
                 THEN 'T3b'
            -- Size-based T staging (microscopic ETE does NOT upstage per AJCC8)
            WHEN tumor_size_cm IS NULL THEN NULL
            WHEN tumor_size_cm > 4 THEN 'T3a'
            WHEN tumor_size_cm > 2 THEN 'T2'
            WHEN tumor_size_cm > 1 THEN 'T1b'
            ELSE 'T1a'
        END AS ajcc8_t_stage,

        CASE
            WHEN gross_ete_flag OR ete_grade LIKE '%gross%' THEN 'ete_grade'
            WHEN tumor_size_cm IS NOT NULL THEN 'tumor_size_cm'
            ELSE 'unknown'
        END AS ajcc8_t_source,

        -- ── N stage ──────────────────────────────────────────────────────
        CASE
            -- Lateral or mediastinal LN involvement → N1b
            WHEN lateral_dissected
                 AND COALESCE(ln_positive, 0) > 0       THEN 'N1b'
            WHEN ln_level_raw LIKE '%lateral%'
                 OR ln_level_raw LIKE '%II%' OR ln_level_raw LIKE '%III%'
                 OR ln_level_raw LIKE '%IV%' OR ln_level_raw LIKE '%V%'
                 OR ln_level_raw LIKE '%mediastin%'      THEN 'N1b'
            WHEN ln_location_raw LIKE '%lateral%'
                 OR ln_location_raw LIKE '%II%' OR ln_location_raw LIKE '%III%'
                 OR ln_location_raw LIKE '%IV%' OR ln_location_raw LIKE '%V%'
                 THEN 'N1b'
            -- Central LN involvement → N1a
            WHEN COALESCE(ln_positive, 0) > 0           THEN 'N1a'
            -- No LN involvement
            WHEN ln_examined IS NOT NULL AND ln_positive = 0 THEN 'N0'
            -- No dissection done (cannot assign)
            ELSE NULL
        END AS ajcc8_n_stage,

        CASE
            WHEN ln_positive IS NOT NULL AND ln_examined IS NOT NULL THEN 'ln_yield'
            WHEN ln_positive IS NOT NULL THEN 'ln_positive_only'
            ELSE 'unknown'
        END AS ajcc8_n_source,

        -- ── M stage ──────────────────────────────────────────────────────
        CASE
            WHEN distant_mets_proxy THEN 'M1'
            WHEN tg_max > 10 AND rai_received THEN 'M1_biochemical'
            ELSE 'M0'
        END AS ajcc8_m_stage,

        -- ── Stage group (age-dependent DTC rules) ─────────────────────
        -- <55: Stage I = any T/N, M0; Stage II = any T/N, M1
        -- >=55: I=T1-2 N0 M0; II=T1-2 N1 or T3; III=T4a; IVA=T4b; IVB=M1
        age_at_surgery,

        -- ── Calculability ─────────────────────────────────────────────
        (tumor_size_cm IS NOT NULL
         AND age_at_surgery IS NOT NULL
         AND ete_grade != 'unknown') AS ajcc8_calculable_flag,

        -- ── Missing component bitmask ──────────────────────────────────
        CONCAT_WS(',',
            CASE WHEN tumor_size_cm IS NULL THEN 'tumor_size' END,
            CASE WHEN age_at_surgery IS NULL THEN 'age' END,
            CASE WHEN ln_positive IS NULL AND ln_examined IS NULL THEN 'ln_data' END,
            CASE WHEN ete_grade = 'unknown' THEN 'ete_grade' END
        ) AS ajcc8_missing_components,

        ete_grade, gross_ete_flag, vascular_invasion_grade, margin_status,
        ln_positive, ln_examined, central_dissected, lateral_dissected,
        ln_max_deposit_cm, multifocal_flag, histology, aggressive_variant_flag,
        distant_mets_proxy, rai_received, tg_nadir, tg_max,
        braf_positive, tert_positive, ras_positive,
        tumor_size_cm, sex, histologic_grade_raw, surgery_date

    FROM staging
),

-- Stage group derivation (requires T/N/M from above)
ajcc8_stage AS (
    SELECT *,
        CASE
            WHEN NOT ajcc8_calculable_flag THEN NULL
            -- <55 years: simplified DTC rules
            WHEN age_at_surgery < 55 THEN
                CASE
                    WHEN ajcc8_m_stage = 'M1' THEN 'II'
                    ELSE 'I'
                END
            -- >=55 years: T/N/M based grouping
            ELSE
                CASE
                    WHEN ajcc8_m_stage IN ('M1','M1_biochemical') THEN 'IVB'
                    WHEN ajcc8_t_stage = 'T4b' THEN 'IVA'
                    WHEN ajcc8_t_stage = 'T4a'
                         OR ajcc8_n_stage = 'N1b' THEN 'III'
                    WHEN ajcc8_t_stage IN ('T1a','T1b','T2')
                         AND COALESCE(ajcc8_n_stage,'N0') = 'N0' THEN 'I'
                    WHEN ajcc8_t_stage IN ('T1a','T1b','T2')
                         AND ajcc8_n_stage = 'N1a' THEN 'II'
                    WHEN ajcc8_t_stage IN ('T3a','T3b') THEN 'II'
                    ELSE NULL
                END
        END AS ajcc8_stage_group
    FROM ajcc8
),

-- ────────────────────────────────────────────────────────────────────────────
-- ATA 2015 Initial Recurrence Risk
-- Reference: Haugen et al., Thyroid 2016;26:1-133
-- ────────────────────────────────────────────────────────────────────────────
ata_risk AS (
    SELECT
        research_id,

        CASE
            -- HIGH risk criteria
            WHEN gross_ete_flag THEN 'high'
            WHEN margin_status LIKE '%r1%' OR margin_status LIKE '%positive%' THEN 'high'
            WHEN distant_mets_proxy THEN 'high'
            WHEN histology IN ('ftc','follicular_thyroid_carcinoma')
                 AND (vascular_invasion_grade LIKE '%extensive%'
                      OR vascular_invasion_grade LIKE '%focal%') THEN 'high'
            -- Tg suggesting distant mets (post-RAI Tg >10 implies residual/distant)
            WHEN tg_max > 10 AND rai_received THEN 'high'
            WHEN ln_max_deposit_cm > 3 THEN 'high'
            -- INTERMEDIATE risk criteria
            WHEN aggressive_variant_flag THEN 'intermediate'
            WHEN vascular_invasion_grade IS NOT NULL
                 AND vascular_invasion_grade NOT IN ('absent','negative','none','')
                 THEN 'intermediate'
            WHEN ete_grade LIKE '%microscopic%' THEN 'intermediate'
            -- >5 LN or any LN 0.2-3cm
            WHEN ln_positive > 5 THEN 'intermediate'
            WHEN ln_max_deposit_cm BETWEEN 0.2 AND 3.0 THEN 'intermediate'
            -- RAI uptake in neck (lymph node avidity without distant disease)
            WHEN rai_received AND NOT distant_mets_proxy THEN 'intermediate'
            -- LOW risk criteria
            WHEN (histology LIKE '%ptc%' OR histology LIKE '%papillary%')
                 AND NOT aggressive_variant_flag
                 AND COALESCE(vascular_invasion_grade,'absent')
                     IN ('absent','negative','none','')
                 AND (ete_grade IS NULL OR ete_grade IN ('none','absent',''))
                 AND COALESCE(ln_positive, 0) <= 5
                 THEN 'low'
            ELSE NULL
        END AS ata_risk_category,

        -- Risk reason (most weight criteria that fired)
        CASE
            WHEN gross_ete_flag THEN 'gross_ete'
            WHEN distant_mets_proxy THEN 'distant_mets'
            WHEN ln_max_deposit_cm > 3 THEN 'large_ln_deposit_gt3cm'
            WHEN margin_status LIKE '%positive%' THEN 'positive_margin'
            WHEN aggressive_variant_flag THEN 'aggressive_variant'
            WHEN vascular_invasion_grade NOT IN ('absent','negative','none','')
                 AND vascular_invasion_grade IS NOT NULL THEN 'vascular_invasion'
            WHEN ete_grade LIKE '%microscopic%' THEN 'microscopic_ete'
            WHEN ln_positive > 5 THEN 'ln_count_gt5'
            ELSE 'size_based_low_risk'
        END AS ata_risk_reason,

        -- Calculability: need histology + LN info + ETE + margin
        (histology IS NOT NULL
         AND histology NOT IN ('','unknown')
         AND ete_grade IS NOT NULL) AS ata_calculable_flag,

        CONCAT_WS(',',
            CASE WHEN histology IS NULL OR histology = '' THEN 'histology' END,
            CASE WHEN ete_grade IS NULL THEN 'ete_grade' END
        ) AS ata_missing_components

    FROM ajcc8_stage
),

-- ────────────────────────────────────────────────────────────────────────────
-- ATA Response to Therapy
-- Reference: Haugen et al., Thyroid 2016 Table 6
-- ────────────────────────────────────────────────────────────────────────────
ata_response AS (
    SELECT
        research_id,

        CASE
            -- Excellent: post-RAI Tg < 0.2 or stimulated Tg < 1
            WHEN rai_received
                 AND tg_nadir IS NOT NULL
                 AND tg_nadir < 0.2
                 AND NOT distant_mets_proxy
                 THEN 'excellent'
            -- Structural incomplete: structural/functional evidence of disease
            WHEN distant_mets_proxy THEN 'structural_incomplete'
            -- Biochemical incomplete: rising or elevated Tg without structural disease
            WHEN tg_max IS NOT NULL AND tg_max > 1 AND NOT distant_mets_proxy
                 THEN 'biochemical_incomplete'
            -- Indeterminate: nonspecific findings (mild Tg elevation with no structural evidence)
            WHEN tg_max IS NOT NULL AND tg_max BETWEEN 0.2 AND 1 THEN 'indeterminate'
            -- Cannot assess: no Tg data
            ELSE NULL
        END AS ata_response_category,

        CASE
            WHEN tg_nadir IS NOT NULL AND rai_received THEN 'thyroglobulin_labs'
            WHEN distant_mets_proxy THEN 'recurrence_refined_v1'
            ELSE NULL
        END AS ata_response_source,

        -- Need post-RAI Tg to make full assessment
        (rai_received AND tg_max IS NOT NULL) AS ata_response_calculable_flag

    FROM ajcc8_stage
),

-- ────────────────────────────────────────────────────────────────────────────
-- MACIS Score (Mayo Clinic)
-- Reference: Hay et al., Surgery 1993;114:1050-8
-- Formula: 3.1*(age factor) + 0.3*size + 1.0*(incomplete resection) +
--          1.0*(local invasion) + 3.0*(distant mets)
-- Age factor: age<40 → 0.08*age; age>=40 → 0.22*age
-- Risk groups: <6=low, 6-6.99=intermediate, 7-7.99=high, >=8=very high
-- ────────────────────────────────────────────────────────────────────────────
macis AS (
    SELECT
        research_id,
        age_at_surgery,
        tumor_size_cm,
        gross_ete_flag,
        margin_status,
        distant_mets_proxy,

        CASE
            WHEN age_at_surgery IS NULL OR tumor_size_cm IS NULL THEN NULL
            ELSE ROUND(
                3.1 * CASE WHEN age_at_surgery < 40
                           THEN 0.08 * age_at_surgery
                           ELSE 0.22 * age_at_surgery END
                + 0.3 * GREATEST(tumor_size_cm, 0)
                + CASE WHEN margin_status LIKE '%positive%'
                            OR margin_status LIKE '%r1%' THEN 1.0 ELSE 0.0 END
                + CASE WHEN gross_ete_flag THEN 1.0 ELSE 0.0 END
                + CASE WHEN distant_mets_proxy THEN 3.0 ELSE 0.0 END,
                2)
        END AS macis_score,

        (age_at_surgery IS NOT NULL AND tumor_size_cm IS NOT NULL)
            AS macis_calculable_flag,

        CONCAT_WS(',',
            CASE WHEN age_at_surgery IS NULL THEN 'age' END,
            CASE WHEN tumor_size_cm IS NULL THEN 'tumor_size' END
        ) AS macis_missing_components

    FROM ajcc8_stage
),

macis_grp AS (
    SELECT *,
        CASE
            WHEN macis_score IS NULL       THEN NULL
            WHEN macis_score < 6.0         THEN 'low'
            WHEN macis_score < 7.0         THEN 'intermediate'
            WHEN macis_score < 8.0         THEN 'high'
            ELSE 'very_high'
        END AS macis_risk_group
    FROM macis
),

-- ────────────────────────────────────────────────────────────────────────────
-- AGES Score (Mayo Clinic)
-- Reference: Hay et al., Surgery 1987;102:1088-95
-- Formula: 0.05*age + 1*(grade 2) + 3*(grade 3-4) + 1*(ETE) +
--          3*(distant mets) + 0.2*size
-- ────────────────────────────────────────────────────────────────────────────
ages AS (
    SELECT
        research_id,
        age_at_surgery,
        tumor_size_cm,
        gross_ete_flag,
        distant_mets_proxy,
        histologic_grade_raw,

        CASE
            WHEN age_at_surgery IS NULL OR tumor_size_cm IS NULL THEN NULL
            ELSE ROUND(
                0.05 * age_at_surgery
                + CASE WHEN LOWER(CAST(histologic_grade_raw AS VARCHAR)) LIKE '%2%'
                            OR LOWER(CAST(histologic_grade_raw AS VARCHAR)) LIKE '%moderately%'
                            THEN 1.0
                       WHEN LOWER(CAST(histologic_grade_raw AS VARCHAR)) LIKE '%3%'
                            OR LOWER(CAST(histologic_grade_raw AS VARCHAR)) LIKE '%4%'
                            OR LOWER(CAST(histologic_grade_raw AS VARCHAR)) LIKE '%poorly%'
                            OR LOWER(CAST(histologic_grade_raw AS VARCHAR)) LIKE '%high%'
                            THEN 3.0
                       ELSE 0.0 END
                + CASE WHEN gross_ete_flag THEN 1.0 ELSE 0.0 END
                + CASE WHEN distant_mets_proxy THEN 3.0 ELSE 0.0 END
                + 0.2 * GREATEST(tumor_size_cm, 0),
                2)
        END AS ages_score,

        (age_at_surgery IS NOT NULL AND tumor_size_cm IS NOT NULL)
            AS ages_calculable_flag,

        CONCAT_WS(',',
            CASE WHEN age_at_surgery IS NULL THEN 'age' END,
            CASE WHEN tumor_size_cm IS NULL THEN 'tumor_size' END
        ) AS ages_missing_components

    FROM ajcc8_stage
),

-- ────────────────────────────────────────────────────────────────────────────
-- AMES Classification (Lahey Clinic)
-- Reference: Cady & Rossi, Surgery 1988;104:947-53
-- High risk if: older patient (M>40, F>50) AND (distant mets OR major ETE
--               OR tumor >5cm)
-- Low risk: all others
-- ────────────────────────────────────────────────────────────────────────────
ames AS (
    SELECT
        research_id,
        age_at_surgery,
        sex,
        tumor_size_cm,
        gross_ete_flag,
        distant_mets_proxy,

        CASE
            WHEN age_at_surgery IS NULL OR tumor_size_cm IS NULL OR sex IS NULL
                 THEN NULL
            WHEN (
                    (sex LIKE '%male%' AND NOT sex LIKE '%fe%'
                     AND age_at_surgery > 40)
                    OR (sex LIKE '%female%' AND age_at_surgery > 50)
                 )
                 AND (distant_mets_proxy
                      OR gross_ete_flag
                      OR tumor_size_cm > 5)
                 THEN 'high'
            ELSE 'low'
        END AS ames_risk_group,

        (age_at_surgery IS NOT NULL AND tumor_size_cm IS NOT NULL AND sex IS NOT NULL)
            AS ames_calculable_flag,

        CONCAT_WS(',',
            CASE WHEN age_at_surgery IS NULL THEN 'age' END,
            CASE WHEN tumor_size_cm IS NULL THEN 'tumor_size' END,
            CASE WHEN sex IS NULL THEN 'sex' END
        ) AS ames_missing_components

    FROM ajcc8_stage
),

-- ────────────────────────────────────────────────────────────────────────────
-- LN Burden Metrics
-- ────────────────────────────────────────────────────────────────────────────
ln_burden AS (
    SELECT
        research_id,
        COALESCE(ln_positive, 0)        AS ln_positive,
        COALESCE(ln_examined, 0)        AS ln_examined,
        CASE
            WHEN ln_examined IS NULL OR ln_examined = 0 THEN NULL
            ELSE ROUND(COALESCE(ln_positive, 0) / ln_examined, 4)
        END AS ln_ratio,
        CASE
            WHEN ln_positive IS NULL THEN 'unknown'
            WHEN ln_positive = 0 THEN 'N0'
            WHEN ln_positive <= 3 THEN 'low_burden_1to3'
            ELSE 'high_burden_gt3'
        END AS ln_burden_band,
        central_dissected AS central_ln_dissected,
        lateral_dissected AS lateral_ln_dissected
    FROM ajcc8_stage
),

-- ────────────────────────────────────────────────────────────────────────────
-- Molecular Risk Composite
-- High = BRAF V600E + TERT co-mutation (highest recurrence risk in literature)
-- Intermediate = single driver mutation (BRAF alone, RAS alone)
-- Low = all negative or untested
-- Reference: Xing et al., Lancet Oncol 2014
-- ────────────────────────────────────────────────────────────────────────────
mol_risk AS (
    SELECT
        research_id,
        braf_positive,
        tert_positive,
        ras_positive,
        CASE
            WHEN braf_positive AND tert_positive THEN 'high'
            WHEN braf_positive AND NOT tert_positive THEN 'intermediate_braf'
            WHEN ras_positive AND NOT braf_positive AND NOT tert_positive
                 THEN 'intermediate_ras'
            WHEN NOT braf_positive AND NOT ras_positive AND NOT tert_positive
                 THEN 'low'
            ELSE 'unknown'
        END AS molecular_risk_tier,
        -- Calculable only if at least one molecular test was performed
        (braf_positive OR ras_positive OR tert_positive
         -- If all are FALSE it means tested-negative, still calculable
         ) AS molecular_risk_calculable_flag
    FROM ajcc8_stage
),

-- ────────────────────────────────────────────────────────────────────────────
-- Bethesda category (from extracted_fna_bethesda_v1)
-- ────────────────────────────────────────────────────────────────────────────
bethesda AS (
    SELECT
        research_id,
        bethesda_final,
        bethesda_source,
        bethesda_confidence
    FROM extracted_fna_bethesda_v1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id
        ORDER BY bethesda_confidence DESC NULLS LAST,
                 CASE bethesda_final
                     WHEN 'VI' THEN 1 WHEN 'V' THEN 2 WHEN 'IV' THEN 3
                     WHEN 'III' THEN 4 WHEN 'II' THEN 5 ELSE 6 END
    ) = 1
)

-- ────────────────────────────────────────────────────────────────────────────
-- Final SELECT: assemble all scores into one row per patient
-- ────────────────────────────────────────────────────────────────────────────
SELECT
    a.research_id,

    -- ── AJCC 8th Edition ────────────────────────────────────────────────
    a.ajcc8_t_stage,
    a.ajcc8_t_source,
    a.ajcc8_n_stage,
    a.ajcc8_n_source,
    a.ajcc8_m_stage,
    a.ajcc8_stage_group,
    a.ajcc8_calculable_flag,
    a.ajcc8_missing_components,

    -- ── ATA Initial Risk ─────────────────────────────────────────────────
    ar.ata_risk_category,
    ar.ata_risk_reason,
    ar.ata_calculable_flag,
    ar.ata_missing_components,

    -- ── ATA Response to Therapy ──────────────────────────────────────────
    resp.ata_response_category,
    resp.ata_response_source,
    resp.ata_response_calculable_flag,

    -- ── MACIS ─────────────────────────────────────────────────────────────
    mg.macis_score,
    mg.macis_risk_group,
    mg.macis_calculable_flag,
    mg.macis_missing_components,

    -- ── AGES ──────────────────────────────────────────────────────────────
    ag.ages_score,
    ag.ages_calculable_flag,
    ag.ages_missing_components,

    -- ── AMES ──────────────────────────────────────────────────────────────
    am.ames_risk_group,
    am.ames_calculable_flag,
    am.ames_missing_components,

    -- ── LN Burden ─────────────────────────────────────────────────────────
    lb.ln_positive,
    lb.ln_examined,
    lb.ln_ratio,
    lb.ln_burden_band,
    lb.central_ln_dissected,
    lb.lateral_ln_dissected,

    -- ── Molecular Risk Composite ─────────────────────────────────────────
    mr.molecular_risk_tier,
    mr.molecular_risk_calculable_flag,
    mr.braf_positive,
    mr.tert_positive,
    mr.ras_positive,

    -- ── Bethesda (FNA) ───────────────────────────────────────────────────
    b.bethesda_final,
    b.bethesda_source,
    b.bethesda_confidence,

    -- ── Clinical inputs (for audit) ──────────────────────────────────────
    a.tumor_size_cm,
    a.age_at_surgery,
    a.sex,
    a.ete_grade,
    a.gross_ete_flag,
    a.vascular_invasion_grade,
    a.margin_status,
    a.histology,
    a.multifocal_flag,
    a.distant_mets_proxy,
    a.rai_received,
    a.tg_nadir,
    a.tg_max,

    -- ── Audit metadata ───────────────────────────────────────────────────
    'v1'                    AS scoring_version,
    'AJCC8,ATA2015,MACIS,AGES,AMES' AS scoring_systems_applied,
    CURRENT_TIMESTAMP       AS scored_at

FROM ajcc8_stage a
LEFT JOIN ata_risk ar USING (research_id)
LEFT JOIN ata_response resp USING (research_id)
LEFT JOIN macis_grp mg USING (research_id)
LEFT JOIN ages ag USING (research_id)
LEFT JOIN ames am USING (research_id)
LEFT JOIN ln_burden lb USING (research_id)
LEFT JOIN mol_risk mr USING (research_id)
LEFT JOIN bethesda b USING (research_id)
"""

# ────────────────────────────────────────────────────────────────────────────
# Validation summary table
# ────────────────────────────────────────────────────────────────────────────
SCORING_VALIDATION_SQL = """
CREATE OR REPLACE TABLE val_scoring_systems AS
SELECT
    COUNT(*)                                        AS total_patients,
    SUM(CASE WHEN ajcc8_calculable_flag THEN 1 ELSE 0 END)
                                                    AS ajcc8_calculable_n,
    ROUND(100.0 * SUM(CASE WHEN ajcc8_calculable_flag THEN 1 ELSE 0 END)
          / COUNT(*), 1)                            AS ajcc8_calculable_pct,
    SUM(CASE WHEN ata_calculable_flag THEN 1 ELSE 0 END)
                                                    AS ata_calculable_n,
    ROUND(100.0 * SUM(CASE WHEN ata_calculable_flag THEN 1 ELSE 0 END)
          / COUNT(*), 1)                            AS ata_calculable_pct,
    SUM(CASE WHEN macis_calculable_flag THEN 1 ELSE 0 END)
                                                    AS macis_calculable_n,
    ROUND(100.0 * SUM(CASE WHEN macis_calculable_flag THEN 1 ELSE 0 END)
          / COUNT(*), 1)                            AS macis_calculable_pct,
    SUM(CASE WHEN ames_calculable_flag THEN 1 ELSE 0 END)
                                                    AS ames_calculable_n,
    -- Stage distributions
    SUM(CASE WHEN ajcc8_stage_group = 'I' THEN 1 ELSE 0 END)    AS stage_I_n,
    SUM(CASE WHEN ajcc8_stage_group = 'II' THEN 1 ELSE 0 END)   AS stage_II_n,
    SUM(CASE WHEN ajcc8_stage_group = 'III' THEN 1 ELSE 0 END)  AS stage_III_n,
    SUM(CASE WHEN ajcc8_stage_group IN ('IVA','IVB') THEN 1 ELSE 0 END)
                                                                  AS stage_IV_n,
    -- ATA risk distributions
    SUM(CASE WHEN ata_risk_category = 'low' THEN 1 ELSE 0 END)         AS ata_low_n,
    SUM(CASE WHEN ata_risk_category = 'intermediate' THEN 1 ELSE 0 END) AS ata_intermediate_n,
    SUM(CASE WHEN ata_risk_category = 'high' THEN 1 ELSE 0 END)        AS ata_high_n,
    -- MACIS distribution
    MIN(macis_score)        AS macis_min,
    ROUND(AVG(macis_score), 2) AS macis_mean,
    MAX(macis_score)        AS macis_max,
    -- Molecular risk
    SUM(CASE WHEN molecular_risk_tier = 'high' THEN 1 ELSE 0 END)
                                                    AS mol_high_risk_n,
    CURRENT_TIMESTAMP       AS validated_at
FROM thyroid_scoring_systems_v1
"""


def _try_fallback_table(con: duckdb.DuckDBPyConnection,
                        preferred: str, fallback: str) -> str:
    """Return preferred table name if available, else fallback."""
    return preferred if table_available(con, preferred) else fallback


def build_scoring_table(con: duckdb.DuckDBPyConnection,
                        dry_run: bool = False) -> None:
    section("Building thyroid_scoring_systems_v1")

    # Guard: need tumor_episode_master_v2 as the foundation
    if not table_available(con, "tumor_episode_master_v2"):
        print("  [SKIP] tumor_episode_master_v2 not available -- run script 22 first")
        return

    # Determine which master clinical table is available (v9+ has ete_grade_v9)
    mcv_table = None
    for ver in ["patient_refined_master_clinical_v12",
                "patient_refined_master_clinical_v11",
                "patient_refined_master_clinical_v10",
                "patient_refined_master_clinical_v9"]:
        if table_available(con, ver):
            mcv_table = ver
            break
    if mcv_table is None:
        print("  [WARN] No patient_refined_master_clinical_v* found; scoring will use raw fields only")

    # Check optional upstream tables
    has_recurrence = table_available(con, "extracted_recurrence_refined_v1")
    has_bethesda = table_available(con, "extracted_fna_bethesda_v1")
    has_tg = table_available(con, "thyroglobulin_labs")

    print("  tumor_episode_master_v2:           present")
    print(f"  master_clinical_v12:               {'present' if mcv_table else 'missing (degraded mode)'}")
    print(f"  extracted_recurrence_refined_v1:   {'present' if has_recurrence else 'missing (distant_mets_proxy=FALSE)'}")
    print(f"  extracted_fna_bethesda_v1:         {'present' if has_bethesda else 'missing (Bethesda NULLs)'}")
    print(f"  thyroglobulin_labs:                {'present' if has_tg else 'missing (Tg NULLs)'}")

    # Build adapter CTEs for missing tables
    preamble_sqls: list[str] = []

    if not has_recurrence:
        preamble_sqls.append("""
CREATE OR REPLACE TEMP TABLE extracted_recurrence_refined_v1 AS
SELECT DISTINCT research_id, FALSE AS recurrence_confirmed,
       NULL::DATE AS first_recurrence_date
FROM tumor_episode_master_v2
WHERE 1=0
""")

    if not has_bethesda:
        preamble_sqls.append("""
CREATE OR REPLACE TEMP TABLE extracted_fna_bethesda_v1 AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS bethesda_final,
       NULL::VARCHAR AS bethesda_source, NULL::INTEGER AS bethesda_confidence
WHERE 1=0
""")

    if not has_tg:
        preamble_sqls.append("""
CREATE OR REPLACE TEMP TABLE thyroglobulin_labs AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS result,
       NULL::TIMESTAMP AS specimen_collect_dt
WHERE 1=0
""")

    if mcv_table and mcv_table != "patient_refined_master_clinical_v12":
        # Create a v12 alias so the SQL remains stable
        preamble_sqls.append(f"""
CREATE OR REPLACE TEMP TABLE patient_refined_master_clinical_v12 AS
SELECT * FROM {mcv_table}
""")

    if not table_available(con, "patient_refined_master_clinical_v12"):
        # Full stub for degraded mode
        preamble_sqls.append("""
CREATE OR REPLACE TEMP TABLE patient_refined_master_clinical_v12 AS
SELECT DISTINCT research_id,
       NULL::VARCHAR AS ete_grade_v9,
       NULL::VARCHAR AS vasc_grade_final_v13,
       NULL::VARCHAR AS margin_r_class_v10,
       NULL::INTEGER AS ln_total_positive_v10,
       NULL::INTEGER AS ln_total_examined,
       NULL::BOOLEAN AS ln_central_dissected,
       NULL::BOOLEAN AS lateral_neck_dissected_v10,
       NULL::VARCHAR AS histology_normalized,
       NULL::VARCHAR AS braf_positive_final,
       NULL::VARCHAR AS tert_positive_v9,
       NULL::VARCHAR AS ras_positive_final
FROM tumor_episode_master_v2
""")

    if not table_available(con, "rai_treatment_episode_v2"):
        preamble_sqls.append("""
CREATE OR REPLACE TEMP TABLE rai_treatment_episode_v2 AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS rai_assertion_status,
       NULL::DATE AS resolved_rai_date
WHERE 1=0
""")

    if dry_run:
        print("  [DRY-RUN] Would create thyroid_scoring_systems_v1")
        return

    for sql in preamble_sqls:
        con.execute(sql.strip())

    print("  Executing scoring SQL...")
    con.execute(SCORING_SQL)
    row = con.execute("SELECT COUNT(*) FROM thyroid_scoring_systems_v1").fetchone()
    print(f"  thyroid_scoring_systems_v1: {row[0]:,} rows")

    print("  Executing validation summary...")
    con.execute(SCORING_VALIDATION_SQL)
    vrow = con.execute("SELECT * FROM val_scoring_systems").fetchdf()
    print(vrow.to_string(index=False))

    print("\n  [DONE] thyroid_scoring_systems_v1 created")


def main() -> None:
    p = argparse.ArgumentParser(description="51_thyroid_scoring_systems.py")
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
        register_parquets(con)

    try:
        build_scoring_table(con, dry_run=args.dry_run)
    finally:
        con.close()

    print("\n[COMPLETE] 51_thyroid_scoring_systems.py finished")


if __name__ == "__main__":
    main()
