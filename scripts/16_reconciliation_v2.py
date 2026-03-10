#!/usr/bin/env python3
"""
16_reconciliation_v2.py — Episode Reconciliation & Validation Framework

Builds on Phase 1 date association work (15_date_association_audit.py).
Creates upgraded reconciliation views, audit surfaces, and analytics layers.

Phases:
  A. Histology reconciliation upgrade (histology_reconciliation_v2)
  B. Molecular episode reconciliation upgrade (molecular_episode_v2,
     molecular_unresolved_audit_mv)
  C. RAI episode model hardening (rai_episode_v2, rai_unresolved_audit_mv)
  D. Missing timeline rescue layer (timeline_rescue_mv,
     timeline_unresolved_summary_mv)
  E. Validation framework upgrade (validation_failures_v2,
     patient_validation_rollup_mv)
  F. Streamlit/analytics readiness (patient_master_timeline_v2,
     patient_reconciliation_summary_v, patient_episode_audit_v)

Modes:
  --local : Uses local DuckDB with parquets from processed/ (default)
  --md    : Uses MotherDuck (requires MOTHERDUCK_TOKEN)

Run after 15_date_association_audit.py.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "processed"
DB_PATH = ROOT / "thyroid_master.duckdb"
SQL_OUT = ROOT / "scripts" / "16_reconciliation_v2_views.sql"

sys.path.insert(0, str(ROOT))

REQUIRED_TABLES = [
    "path_synoptics",
    "tumor_pathology",
    "note_entities_staging",
    "note_entities_genetics",
    "note_entities_medications",
    "note_entities_procedures",
    "note_entities_complications",
    "note_entities_problem_list",
    "molecular_testing",
    "fna_history",
    "clinical_notes_long",
]

OPTIONAL_TABLES = [
    "benign_pathology",
    "operative_details",
    "master_timeline",
]


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def register_parquets(con: duckdb.DuckDBPyConnection) -> list[str]:
    registered: list[str] = []
    for tbl in REQUIRED_TABLES + OPTIONAL_TABLES:
        pq = PROCESSED / f"{tbl}.parquet"
        if pq.exists():
            con.execute(
                f"CREATE OR REPLACE TABLE {tbl} AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  Registered {tbl:<45} {cnt:>8,} rows")
            registered.append(tbl)
        else:
            try:
                con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
                registered.append(tbl)
                print(f"  Existing  {tbl:<45}")
            except Exception:
                print(f"  SKIP      {tbl:<45} (not available)")
    return registered


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def discover_genetics_date_col(con: duckdb.DuckDBPyConnection) -> str | None:
    try:
        con.execute("SELECT 1 FROM molecular_testing LIMIT 1")
    except Exception:
        return None
    cols = con.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'molecular_testing'
          AND (column_name ILIKE '%date%' OR column_name ILIKE '%year%')
    """).fetchall()
    for (col_name,) in cols:
        valid_ct = con.execute(f"""
            SELECT COUNT(*) FROM molecular_testing
            WHERE "{col_name}" IS NOT NULL
              AND CAST("{col_name}" AS VARCHAR) NOT IN ('x','X','','None','maybe?')
              AND TRY_CAST("{col_name}" AS DATE) IS NOT NULL
        """).fetchone()[0]
        if valid_ct > 0:
            return col_name
    for (col_name,) in cols:
        year_ct = con.execute(f"""
            SELECT COUNT(*) FROM molecular_testing
            WHERE "{col_name}" IS NOT NULL
              AND regexp_matches(CAST("{col_name}" AS VARCHAR), '^[0-9]{{4}}$')
        """).fetchone()[0]
        if year_ct > 0:
            return col_name
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE A — Histology Reconciliation V2
# ═══════════════════════════════════════════════════════════════════════════

HISTOLOGY_RECON_V2_SQL = """
-- Phase A: Histology Reconciliation V2
-- Reconciles pathology data across path_synoptics, tumor_pathology, and
-- note_entities_staging. Adds histology/variant normalization, laterality
-- derivation, multifocality, and comprehensive discordance flags.
-- Episode-aware via op_seq from path_synoptics.
--
-- Histology normalization mapping:
--   PTC_classic, PTC_follicular_variant, PTC_tall_cell, PTC_hobnail,
--   PTC_diffuse_sclerosing, PTC_columnar, FTC, HCC_oncocytic, NIFTP,
--   MTC, ATC, PDTC, adenoma, hyperplasia, benign, other
CREATE OR REPLACE VIEW histology_reconciliation_v2 AS
WITH ps_tumors AS (
    SELECT
        ps.research_id,
        TRY_CAST(ps.surg_date AS DATE) AS pathology_date,
        ps.surg_date AS surg_date_raw,
        ps.tumor_1_histologic_type AS source_histology_raw_ps,
        ps.tumor_1_variant AS variant_raw_ps,
        ps.tumor_1_pt AS t_stage_source_path,
        ps.tumor_1_pn AS n_stage_source_path,
        ps.tumor_1_pm AS m_stage_source_path,
        ps.tumor_1_size_greatest_dimension_cm AS largest_focus_cm,
        ps.tumor_1_extrathyroidal_extension,
        ps.tumor_1_margin_status,
        ps.tumor_1_ln_involved,
        ps.tumor_1_ln_examined,
        ps.tumor_1_angioinvasion,
        ps.tumor_1_lymphatic_invasion,
        ps.tumor_1_perineural_invasion,
        ps.tumor_1_capsular_invasion,
        ps.reop,
        ps.thyroid_procedure,
        ROW_NUMBER() OVER (
            PARTITION BY ps.research_id
            ORDER BY TRY_CAST(ps.surg_date AS DATE) ASC NULLS LAST
        ) AS op_seq
    FROM path_synoptics ps
    WHERE ps.surg_date IS NOT NULL AND ps.surg_date != ''
),
tp_data AS (
    SELECT
        tp.research_id,
        tp.histology_1_type AS source_histology_raw_tp,
        tp.tumor_1_histology_variant AS variant_raw_tp,
        tp.histology_1_t_stage_ajcc8 AS t_stage_tp,
        tp.histology_1_n_stage_ajcc8 AS n_stage_tp,
        tp.histology_1_m_stage_ajcc8 AS m_stage_tp,
        tp.histology_1_overall_stage_ajcc8 AS overall_stage_tp,
        tp.histology_1_largest_tumor_cm AS largest_tumor_cm_tp,
        tp.tumor_focality_overall AS multifocality_tp,
        tp.braf_mutation_mentioned,
        tp.ras_mutation_mentioned,
        tp.ret_mutation_mentioned,
        tp.tert_mutation_mentioned
    FROM tumor_pathology tp
),
staging_notes AS (
    SELECT
        CAST(e.research_id AS BIGINT) AS research_id,
        e.entity_type,
        e.entity_value_norm,
        e.confidence,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(e.research_id AS BIGINT), e.entity_type
            ORDER BY e.confidence DESC NULLS LAST,
                     TRY_CAST(COALESCE(e.entity_date, e.note_date) AS DATE)
                         DESC NULLS LAST
        ) AS rn
    FROM note_entities_staging e
    WHERE e.present_or_negated = 'present'
      AND e.entity_type IN ('T_stage','N_stage','M_stage','overall_stage')
),
se_t  AS (SELECT research_id, entity_value_norm AS note_t_stage
           FROM staging_notes WHERE entity_type='T_stage' AND rn=1),
se_n  AS (SELECT research_id, entity_value_norm AS note_n_stage
           FROM staging_notes WHERE entity_type='N_stage' AND rn=1),
se_m  AS (SELECT research_id, entity_value_norm AS note_m_stage
           FROM staging_notes WHERE entity_type='M_stage' AND rn=1),
se_ov AS (SELECT research_id, entity_value_norm AS note_overall_stage
           FROM staging_notes WHERE entity_type='overall_stage' AND rn=1),
-- Intermediate: join raw data
raw_joined AS (
    SELECT
        ps.*,
        tp.source_histology_raw_tp,
        tp.variant_raw_tp,
        tp.t_stage_tp,
        tp.n_stage_tp,
        tp.m_stage_tp,
        tp.overall_stage_tp,
        tp.largest_tumor_cm_tp,
        tp.multifocality_tp,
        tp.braf_mutation_mentioned,
        tp.ras_mutation_mentioned,
        tp.ret_mutation_mentioned,
        tp.tert_mutation_mentioned,
        se_t.note_t_stage,
        se_n.note_n_stage,
        se_m.note_m_stage,
        se_ov.note_overall_stage,
        COALESCE(ps.source_histology_raw_ps,
                 tp.source_histology_raw_tp) AS source_histology_raw
    FROM ps_tumors ps
    LEFT JOIN tp_data tp
        ON CAST(ps.research_id AS VARCHAR) = CAST(tp.research_id AS VARCHAR)
    LEFT JOIN se_t
        ON CAST(ps.research_id AS BIGINT) = se_t.research_id
    LEFT JOIN se_n
        ON CAST(ps.research_id AS BIGINT) = se_n.research_id
    LEFT JOIN se_m
        ON CAST(ps.research_id AS BIGINT) = se_m.research_id
    LEFT JOIN se_ov
        ON CAST(ps.research_id AS BIGINT) = se_ov.research_id
),
-- Normalize histology and variant in a separate CTE for reuse
normalized AS (
    SELECT
        r.*,
        -- Histology normalization
        CASE
            WHEN r.source_histology_raw ILIKE '%papillary%'
                 AND r.source_histology_raw ILIKE '%tall cell%'
                THEN 'PTC_tall_cell'
            WHEN r.source_histology_raw ILIKE '%papillary%'
                 AND r.source_histology_raw ILIKE '%hobnail%'
                THEN 'PTC_hobnail'
            WHEN r.source_histology_raw ILIKE '%papillary%'
                 AND r.source_histology_raw ILIKE '%diffuse sclerosing%'
                THEN 'PTC_diffuse_sclerosing'
            WHEN r.source_histology_raw ILIKE '%papillary%'
                 AND r.source_histology_raw ILIKE '%columnar%'
                THEN 'PTC_columnar'
            WHEN r.source_histology_raw ILIKE '%papillary%'
                 AND (r.source_histology_raw ILIKE '%follicular variant%'
                      OR r.source_histology_raw ILIKE '%follicular-variant%')
                THEN 'PTC_follicular_variant'
            WHEN r.source_histology_raw ILIKE '%papillary%'
                THEN 'PTC_classic'
            WHEN r.source_histology_raw = 'PTC' THEN 'PTC_classic'
            WHEN r.source_histology_raw ILIKE '%niftp%'
                OR r.source_histology_raw
                   ILIKE '%non-invasive follicular thyroid neoplasm%'
                THEN 'NIFTP'
            WHEN (r.source_histology_raw ILIKE '%follicular%carcinoma%'
                  OR r.source_histology_raw = 'FTC')
                 AND r.source_histology_raw NOT ILIKE '%papillary%'
                THEN 'FTC'
            WHEN r.source_histology_raw ILIKE '%hurthle%'
                OR r.source_histology_raw ILIKE '%oncocytic%'
                OR r.source_histology_raw ILIKE '%oxyphilic%'
                OR r.source_histology_raw = 'HCC'
                THEN 'HCC_oncocytic'
            WHEN r.source_histology_raw ILIKE '%medullary%'
                OR r.source_histology_raw = 'MTC'
                THEN 'MTC'
            WHEN r.source_histology_raw ILIKE '%anaplastic%'
                OR r.source_histology_raw ILIKE '%undifferentiated%'
                OR r.source_histology_raw = 'ATC'
                THEN 'ATC'
            WHEN r.source_histology_raw ILIKE '%poorly differentiated%'
                OR r.source_histology_raw = 'PDTC'
                THEN 'PDTC'
            WHEN r.source_histology_raw ILIKE '%adenoma%' THEN 'adenoma'
            WHEN r.source_histology_raw ILIKE '%hyperplasia%'
                OR r.source_histology_raw ILIKE '%goiter%'
                THEN 'hyperplasia'
            WHEN r.source_histology_raw ILIKE '%benign%'
                OR r.source_histology_raw ILIKE '%nodular%'
                THEN 'benign'
            WHEN r.source_histology_raw IS NOT NULL
                 AND TRIM(r.source_histology_raw) != ''
                THEN 'other'
            ELSE NULL
        END AS histology_normalized,
        -- Variant normalization
        CASE
            WHEN COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                 ILIKE '%tall cell%' THEN 'tall_cell'
            WHEN COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                 ILIKE '%hobnail%'
                OR COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                   ILIKE '%micropapillary%' THEN 'hobnail'
            WHEN COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                 ILIKE '%diffuse sclerosing%' THEN 'diffuse_sclerosing'
            WHEN COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                 ILIKE '%columnar%' THEN 'columnar_cell'
            WHEN COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                 ILIKE '%cribriform%'
                OR COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                   ILIKE '%morular%' THEN 'cribriform_morular'
            WHEN COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                 ILIKE '%solid%' THEN 'solid'
            WHEN COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                 ILIKE '%follicular%'
                AND COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                    NOT ILIKE '%papillary%' THEN 'follicular_variant'
            WHEN COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                 ILIKE '%oncocytic%'
                OR COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                   ILIKE '%warthin%'
                OR COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                   ILIKE '%hurthle%' THEN 'oncocytic'
            WHEN COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                 ILIKE '%classic%'
                OR COALESCE(r.variant_raw_ps, r.variant_raw_tp)
                   ILIKE '%conventional%' THEN 'classic'
            WHEN COALESCE(r.variant_raw_ps, r.variant_raw_tp) IS NOT NULL
                 AND TRIM(COALESCE(r.variant_raw_ps, r.variant_raw_tp)) != ''
                THEN COALESCE(r.variant_raw_ps, r.variant_raw_tp)
            ELSE NULL
        END AS variant_normalized,
        -- Laterality from procedure text
        CASE
            WHEN LOWER(CAST(r.thyroid_procedure AS VARCHAR))
                 LIKE '%total thyroidectomy%'
                OR LOWER(CAST(r.thyroid_procedure AS VARCHAR))
                   LIKE '%bilateral%'
                THEN 'bilateral'
            WHEN LOWER(CAST(r.thyroid_procedure AS VARCHAR))
                 LIKE '%right%lobect%'
                AND LOWER(CAST(r.thyroid_procedure AS VARCHAR))
                    NOT LIKE '%left%'
                THEN 'right'
            WHEN LOWER(CAST(r.thyroid_procedure AS VARCHAR))
                 LIKE '%left%lobect%'
                AND LOWER(CAST(r.thyroid_procedure AS VARCHAR))
                    NOT LIKE '%right%'
                THEN 'left'
            WHEN LOWER(CAST(r.thyroid_procedure AS VARCHAR))
                 LIKE '%isthmusect%'
                THEN 'isthmus'
            ELSE NULL
        END AS laterality_path,
        -- Histology normalization for PS source only (for discordance check)
        CASE
            WHEN r.source_histology_raw_ps ILIKE '%papillary%'
                 AND r.source_histology_raw_ps ILIKE '%tall cell%'
                THEN 'PTC_tall_cell'
            WHEN r.source_histology_raw_ps ILIKE '%papillary%'
                THEN 'PTC_classic'
            WHEN r.source_histology_raw_ps = 'PTC' THEN 'PTC_classic'
            WHEN r.source_histology_raw_ps ILIKE '%follicular%carcinoma%'
                OR r.source_histology_raw_ps = 'FTC' THEN 'FTC'
            WHEN r.source_histology_raw_ps ILIKE '%hurthle%'
                OR r.source_histology_raw_ps ILIKE '%oncocytic%'
                OR r.source_histology_raw_ps = 'HCC' THEN 'HCC_oncocytic'
            WHEN r.source_histology_raw_ps ILIKE '%medullary%'
                OR r.source_histology_raw_ps = 'MTC' THEN 'MTC'
            WHEN r.source_histology_raw_ps ILIKE '%anaplastic%'
                OR r.source_histology_raw_ps = 'ATC' THEN 'ATC'
            WHEN r.source_histology_raw_ps ILIKE '%poorly differentiated%'
                OR r.source_histology_raw_ps = 'PDTC' THEN 'PDTC'
            WHEN r.source_histology_raw_ps ILIKE '%adenoma%' THEN 'adenoma'
            WHEN r.source_histology_raw_ps IS NOT NULL
                 AND TRIM(r.source_histology_raw_ps) != '' THEN 'other'
            ELSE NULL
        END AS histology_norm_ps,
        CASE
            WHEN r.source_histology_raw_tp = 'PTC' THEN 'PTC_classic'
            WHEN r.source_histology_raw_tp = 'FTC' THEN 'FTC'
            WHEN r.source_histology_raw_tp = 'MTC' THEN 'MTC'
            WHEN r.source_histology_raw_tp = 'ATC' THEN 'ATC'
            WHEN r.source_histology_raw_tp = 'HCC' THEN 'HCC_oncocytic'
            WHEN r.source_histology_raw_tp = 'PDTC' THEN 'PDTC'
            WHEN r.source_histology_raw_tp ILIKE '%papillary%'
                THEN 'PTC_classic'
            WHEN r.source_histology_raw_tp ILIKE '%follicular%'
                THEN 'FTC'
            WHEN r.source_histology_raw_tp ILIKE '%hurthle%'
                OR r.source_histology_raw_tp ILIKE '%oncocytic%'
                THEN 'HCC_oncocytic'
            WHEN r.source_histology_raw_tp IS NOT NULL
                 AND TRIM(r.source_histology_raw_tp) != '' THEN 'other'
            ELSE NULL
        END AS histology_norm_tp
    FROM raw_joined r
)
SELECT
    n.research_id,
    n.op_seq,
    n.pathology_date,
    n.source_histology_raw_ps,
    n.source_histology_raw_tp,
    n.source_histology_raw,
    n.histology_normalized,
    n.variant_raw_ps,
    n.variant_raw_tp,
    n.variant_normalized,
    -- Tumor behavior from normalized histology
    CASE
        WHEN n.histology_normalized IN ('PTC_classic','PTC_follicular_variant',
             'PTC_columnar','PTC_diffuse_sclerosing','FTC','NIFTP')
            THEN 'well_differentiated'
        WHEN n.histology_normalized IN ('PTC_tall_cell','PTC_hobnail')
            THEN 'aggressive_variant'
        WHEN n.histology_normalized = 'HCC_oncocytic' THEN 'oncocytic'
        WHEN n.histology_normalized = 'MTC' THEN 'medullary'
        WHEN n.histology_normalized = 'ATC' THEN 'anaplastic'
        WHEN n.histology_normalized = 'PDTC' THEN 'poorly_differentiated'
        WHEN n.histology_normalized IN ('adenoma','hyperplasia','benign')
            THEN 'benign'
        ELSE 'unclassified'
    END AS tumor_behavior_category,
    n.t_stage_source_path,
    n.t_stage_tp,
    n.note_t_stage AS t_stage_source_note,
    COALESCE(n.t_stage_source_path, n.t_stage_tp) AS t_stage_reconciled,
    n.n_stage_source_path,
    n.n_stage_tp,
    n.note_n_stage AS n_stage_source_note,
    COALESCE(n.n_stage_source_path, n.n_stage_tp) AS n_stage_reconciled,
    n.m_stage_source_path,
    n.m_stage_tp,
    n.note_m_stage AS m_stage_source_note,
    n.overall_stage_tp,
    n.note_overall_stage,
    n.laterality_path,
    NULL AS laterality_note,
    n.laterality_path AS laterality_reconciled,
    n.multifocality_tp AS multifocality_path,
    n.largest_focus_cm,
    n.tumor_1_extrathyroidal_extension,
    n.tumor_1_margin_status,
    n.tumor_1_angioinvasion,
    n.tumor_1_lymphatic_invasion,
    n.tumor_1_perineural_invasion,
    n.tumor_1_capsular_invasion,
    n.tumor_1_ln_involved,
    n.tumor_1_ln_examined,
    n.reop,
    n.braf_mutation_mentioned,
    n.ras_mutation_mentioned,
    n.ret_mutation_mentioned,
    n.tert_mutation_mentioned,
    CASE
        WHEN n.t_stage_source_path IS NOT NULL THEN 'path_synoptics'
        WHEN n.t_stage_tp IS NOT NULL THEN 'tumor_pathology'
        WHEN n.note_t_stage IS NOT NULL THEN 'note_entities'
        ELSE 'none'
    END AS source_priority_used,
    CASE
        WHEN n.histology_norm_ps IS NOT NULL
             AND n.histology_norm_tp IS NOT NULL
             AND n.histology_norm_ps != n.histology_norm_tp
            THEN TRUE ELSE FALSE
    END AS histology_discordance_flag,
    CASE
        WHEN n.t_stage_source_path IS NOT NULL
             AND n.note_t_stage IS NOT NULL
             AND LOWER(REPLACE(n.t_stage_source_path,' ',''))
              != LOWER(REPLACE(n.note_t_stage,' ',''))
            THEN TRUE ELSE FALSE
    END AS stage_discordance_flag,
    CASE
        WHEN n.n_stage_source_path IS NOT NULL
             AND n.note_n_stage IS NOT NULL
             AND LOWER(REPLACE(n.n_stage_source_path,' ',''))
              != LOWER(REPLACE(n.note_n_stage,' ',''))
            THEN TRUE ELSE FALSE
    END AS n_stage_discordance_flag,
    FALSE AS laterality_discordance_flag,
    CASE
        WHEN n.t_stage_source_path IS NULL
             AND n.t_stage_tp IS NULL
             AND n.note_t_stage IS NULL
            THEN TRUE ELSE FALSE
    END AS unresolved_flag,
    CASE
        WHEN n.t_stage_source_path IS NULL AND n.t_stage_tp IS NULL
            THEN 'path_missing'
        WHEN n.note_t_stage IS NULL AND n.note_n_stage IS NULL
            THEN 'notes_missing'
        ELSE 'both_present'
    END AS reconciliation_status
FROM normalized n;
"""


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE B — Molecular Episode Reconciliation V2
# ═══════════════════════════════════════════════════════════════════════════


def build_molecular_v2_sql(date_col: str | None) -> str:
    """Build molecular_episode_v2 SQL with dynamic date column."""
    mt_date_expr = "NULL::DATE"
    mt_gran_expr = "'none'"
    mt_raw_expr = "NULL"

    if date_col:
        mt_date_expr = f"""CASE
            WHEN TRY_CAST(mt."{date_col}" AS DATE) IS NOT NULL
                THEN TRY_CAST(mt."{date_col}" AS DATE)
            WHEN regexp_matches(CAST(mt."{date_col}" AS VARCHAR), '^\\d{{4}}$')
                THEN TRY_CAST(CAST(mt."{date_col}" AS VARCHAR) || '-07-01' AS DATE)
            ELSE NULL
        END"""
        mt_gran_expr = f"""CASE
            WHEN TRY_CAST(mt."{date_col}" AS DATE) IS NOT NULL THEN 'day'
            WHEN regexp_matches(CAST(mt."{date_col}" AS VARCHAR), '^\\d{{4}}$')
                THEN 'year'
            ELSE 'invalid'
        END"""
        mt_raw_expr = f'CAST(mt."{date_col}" AS VARCHAR)'

    return f"""
-- Phase B: Molecular Episode Reconciliation V2
-- Links molecular_testing, note_entities_genetics, and fna_history into
-- clinically meaningful episodes with gene-specific flags, surgery linkage,
-- and linkage confidence scoring.
CREATE OR REPLACE VIEW molecular_episode_v2 AS
WITH mt_base AS (
    SELECT
        mt.research_id,
        mt.test_index,
        mt.thyroseq_afirma,
        mt.genetic_test_performed,
        mt.genetic_test AS test_name_raw,
        mt.result AS result_summary_raw,
        mt.mutation,
        mt.detailed_findings,
        mt.nodule_info,
        mt.fna_bethesda,
        {mt_date_expr} AS molecular_date,
        {mt_gran_expr} AS molecular_date_granularity,
        {mt_raw_expr} AS specimen_date_raw,
        'molecular_testing' AS molecular_date_source,

        -- Platform normalization
        CASE
            WHEN mt.thyroseq_afirma ILIKE '%thyroseq%' THEN 'ThyroSeq'
            WHEN mt.thyroseq_afirma ILIKE '%afirma%' THEN 'Afirma'
            WHEN mt.genetic_test ILIKE '%thyroseq%' THEN 'ThyroSeq'
            WHEN mt.genetic_test ILIKE '%afirma%' THEN 'Afirma'
            ELSE mt.thyroseq_afirma
        END AS platform_normalized,
        mt.thyroseq_afirma AS platform_raw,

        -- Result category
        CASE
            WHEN mt.result ILIKE '%positive%' OR mt.result ILIKE '%detected%'
                OR mt.result ILIKE '%mutation%identified%'
                THEN 'positive'
            WHEN mt.result ILIKE '%negative%' OR mt.result ILIKE '%not detected%'
                OR mt.result ILIKE '%no mutation%' OR mt.result ILIKE '%benign%'
                THEN 'negative'
            WHEN mt.result ILIKE '%suspicious%' OR mt.result ILIKE '%indeterminate%'
                THEN 'indeterminate'
            WHEN mt.result IS NULL OR TRIM(CAST(mt.result AS VARCHAR)) = ''
                THEN 'missing'
            ELSE 'other'
        END AS result_category_normalized,

        -- Gene-specific flags from structured mutation/result fields
        CASE WHEN COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.result AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%BRAF%' THEN TRUE ELSE FALSE END AS braf_flag,
        CASE WHEN COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.result AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%RAS%' OR COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%NRAS%' OR COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%HRAS%' OR COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%KRAS%'
            THEN TRUE ELSE FALSE END AS ras_flag,
        CASE WHEN COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.result AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%RET%' THEN TRUE ELSE FALSE END AS ret_flag,
        CASE WHEN COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.result AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%TERT%' THEN TRUE ELSE FALSE END AS tert_flag,
        CASE WHEN COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.result AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%NTRK%' THEN TRUE ELSE FALSE END AS ntrk_flag,
        CASE WHEN COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.result AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%ALK%' THEN TRUE ELSE FALSE END AS alk_flag,
        CASE WHEN COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.result AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%TP53%' OR COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%P53%'
            THEN TRUE ELSE FALSE END AS tp53_flag,
        CASE WHEN COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.result AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%PTEN%' THEN TRUE ELSE FALSE END AS pten_flag,
        CASE WHEN COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.result AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%fusion%' OR COALESCE(CAST(mt.mutation AS VARCHAR),'')
            || COALESCE(CAST(mt.detailed_findings AS VARCHAR),'')
            ILIKE '%RET/PTC%'
            THEN TRUE ELSE FALSE END AS fusion_flag,

        ROW_NUMBER() OVER (
            PARTITION BY mt.research_id
            ORDER BY
                CASE WHEN TRY_CAST(mt."{date_col}" AS DATE) IS NOT NULL THEN 0 ELSE 1 END,
                TRY_CAST(mt."{date_col}" AS DATE) ASC NULLS LAST,
                mt.test_index ASC
        ) AS molecular_episode_id
    FROM molecular_testing mt
),
fna_all AS (
    SELECT
        research_id,
        TRY_CAST(fna_date_parsed AS DATE) AS fna_date,
        bethesda,
        fna_index
    FROM fna_history
    WHERE fna_date_parsed IS NOT NULL AND fna_date_parsed != ''
),
ps_surg AS (
    SELECT
        research_id,
        TRY_CAST(surg_date AS DATE) AS surg_date,
        tumor_1_histologic_type,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST
        ) AS op_seq
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
),
-- Link each molecular test to nearest prior FNA
mt_fna_link AS (
    SELECT
        mt.research_id,
        mt.molecular_episode_id,
        mt.molecular_date,
        fna.fna_date AS linked_fna_date,
        fna.bethesda AS linked_fna_bethesda,
        DATE_DIFF('day', fna.fna_date, mt.molecular_date) AS days_molecular_to_fna,
        ROW_NUMBER() OVER (
            PARTITION BY mt.research_id, mt.molecular_episode_id
            ORDER BY ABS(DATE_DIFF('day', fna.fna_date,
                     COALESCE(mt.molecular_date, DATE '2099-01-01'))) ASC
        ) AS fna_rank
    FROM mt_base mt
    LEFT JOIN fna_all fna ON CAST(mt.research_id AS BIGINT) = fna.research_id
),
-- Link each molecular test to nearest surgery (bounded ±730 days)
mt_surg_link AS (
    SELECT
        mt.research_id,
        mt.molecular_episode_id,
        ps.surg_date AS linked_surgery_date,
        ps.tumor_1_histologic_type AS linked_histology,
        DATE_DIFF('day', mt.molecular_date, ps.surg_date) AS days_molecular_to_surgery,
        ROW_NUMBER() OVER (
            PARTITION BY mt.research_id, mt.molecular_episode_id
            ORDER BY ABS(DATE_DIFF('day',
                     COALESCE(mt.molecular_date, DATE '2099-01-01'),
                     ps.surg_date)) ASC
        ) AS surg_rank
    FROM mt_base mt
    LEFT JOIN ps_surg ps ON CAST(mt.research_id AS BIGINT) = ps.research_id
    WHERE mt.molecular_date IS NOT NULL
      AND ABS(DATE_DIFF('day', mt.molecular_date, ps.surg_date)) <= 730
)
SELECT
    mt.research_id,
    mt.molecular_episode_id,
    mt.specimen_date_raw,
    mt.molecular_date,
    mt.molecular_date_granularity,
    mt.molecular_date_source,
    mt.platform_raw,
    mt.platform_normalized,
    mt.test_name_raw,
    mt.result_summary_raw,
    mt.result_category_normalized,
    mt.braf_flag,
    mt.ras_flag,
    mt.ret_flag,
    mt.tert_flag,
    mt.ntrk_flag,
    mt.alk_flag,
    mt.tp53_flag,
    mt.pten_flag,
    mt.fusion_flag,
    CASE WHEN mt.braf_flag OR mt.tert_flag OR mt.tp53_flag
              OR mt.fusion_flag OR mt.alk_flag OR mt.ntrk_flag
        THEN TRUE ELSE FALSE
    END AS high_risk_molecular_flag,
    fl.linked_fna_date,
    fl.linked_fna_bethesda,
    sl.linked_surgery_date,
    sl.linked_surgery_date AS linked_pathology_date,
    sl.linked_histology,
    fl.days_molecular_to_fna,
    sl.days_molecular_to_surgery,
    CASE
        WHEN mt.molecular_date IS NOT NULL AND fl.linked_fna_date IS NOT NULL
            THEN 'date_proximity_fna'
        WHEN mt.molecular_date IS NOT NULL AND sl.linked_surgery_date IS NOT NULL
            THEN 'date_proximity_surgery'
        WHEN mt.molecular_date IS NULL AND fl.linked_fna_date IS NOT NULL
            THEN 'patient_level_fna'
        ELSE 'unlinked'
    END AS linkage_method,
    CASE
        WHEN mt.molecular_date IS NOT NULL AND mt.molecular_date_granularity = 'day'
             AND fl.linked_fna_date IS NOT NULL
             AND ABS(fl.days_molecular_to_fna) <= 90
            THEN 'high'
        WHEN mt.molecular_date IS NOT NULL AND mt.molecular_date_granularity = 'day'
            THEN 'medium'
        WHEN mt.molecular_date IS NOT NULL AND mt.molecular_date_granularity = 'year'
            THEN 'low'
        WHEN mt.molecular_date IS NULL THEN 'none'
        ELSE 'low'
    END AS linkage_confidence,
    CASE
        WHEN mt.molecular_date IS NULL
             AND fl.linked_fna_date IS NULL
             AND sl.linked_surgery_date IS NULL
            THEN TRUE ELSE FALSE
    END AS unresolved_flag
FROM mt_base mt
LEFT JOIN mt_fna_link fl
    ON CAST(mt.research_id AS BIGINT) = CAST(fl.research_id AS BIGINT)
    AND mt.molecular_episode_id = fl.molecular_episode_id
    AND fl.fna_rank = 1
LEFT JOIN mt_surg_link sl
    ON CAST(mt.research_id AS BIGINT) = CAST(sl.research_id AS BIGINT)
    AND mt.molecular_episode_id = sl.molecular_episode_id
    AND sl.surg_rank = 1;
"""


MOLECULAR_UNRESOLVED_SQL = """
-- Phase B: Molecular Unresolved Audit
-- Explains why each unresolved molecular row failed linkage.
CREATE OR REPLACE VIEW molecular_unresolved_audit_mv AS
SELECT
    m.research_id,
    m.molecular_episode_id,
    m.specimen_date_raw,
    m.molecular_date,
    m.molecular_date_granularity,
    m.test_name_raw,
    m.result_summary_raw,
    CASE
        WHEN m.molecular_date IS NULL AND m.specimen_date_raw IS NULL
            THEN 'no_source_date'
        WHEN m.molecular_date IS NULL AND m.specimen_date_raw IS NOT NULL
            THEN 'parse_failure'
        WHEN m.molecular_date IS NOT NULL AND m.linked_fna_date IS NULL
             AND m.linked_surgery_date IS NULL
            THEN 'no_related_episode'
        WHEN m.linkage_confidence = 'none'
            THEN 'patient_missing_anchor_tables'
        ELSE 'unknown'
    END AS unresolved_reason,
    m.linkage_method,
    m.linkage_confidence
FROM molecular_episode_v2 m
WHERE m.unresolved_flag = TRUE
   OR m.linkage_confidence IN ('none', 'low');
"""


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE C — RAI Episode V2
# ═══════════════════════════════════════════════════════════════════════════

RAI_EPISODE_V2_SQL = """
-- Phase C: RAI Episode V2
-- Auditable treatment episode model with normalized terms, mention type
-- classification, nearest-surgery linkage with plausibility bounds.
CREATE OR REPLACE VIEW rai_episode_v2 AS
WITH rai_mentions AS (
    SELECT
        e.research_id,
        e.entity_value_norm,
        e.entity_value_raw,
        e.present_or_negated,
        e.confidence,
        e.entity_date,
        e.note_date,
        TRY_CAST(COALESCE(e.entity_date, e.note_date) AS DATE) AS rai_date,
        CASE
            WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'entity_date'
            WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'note_date'
            ELSE 'unrecoverable'
        END AS rai_date_source,
        CASE
            WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'day'
            WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'day'
            ELSE NULL
        END AS rai_date_granularity,
        e.evidence_span AS rai_mention_text,
        'note_entities_medications' AS source_table,
        'enriched_note_entities_medications' AS source_view,

        -- RAI term normalization
        CASE
            WHEN LOWER(e.entity_value_norm) LIKE '%i-131%'
                OR LOWER(e.entity_value_norm) LIKE '%i131%'
                THEN 'I-131'
            WHEN LOWER(e.entity_value_norm) LIKE '%radioactive iodine%'
                OR LOWER(e.entity_value_norm) LIKE '%rai%'
                THEN 'RAI'
            WHEN LOWER(e.entity_value_norm) LIKE '%thyrogen%'
                THEN 'Thyrogen'
            WHEN LOWER(e.entity_value_norm) LIKE '%iodine%'
                THEN 'iodine_related'
            ELSE e.entity_value_norm
        END AS rai_term_normalized,

        -- Mention type classification
        CASE
            WHEN e.present_or_negated = 'negated' THEN 'negated'
            WHEN LOWER(COALESCE(CAST(e.evidence_span AS VARCHAR), ''))
                LIKE '%will receive%'
                OR LOWER(COALESCE(CAST(e.evidence_span AS VARCHAR), ''))
                LIKE '%plan%'
                OR LOWER(COALESCE(CAST(e.evidence_span AS VARCHAR), ''))
                LIKE '%scheduled%'
                THEN 'planned'
            WHEN LOWER(COALESCE(CAST(e.evidence_span AS VARCHAR), ''))
                LIKE '%history%'
                OR LOWER(COALESCE(CAST(e.evidence_span AS VARCHAR), ''))
                LIKE '%prior%'
                OR LOWER(COALESCE(CAST(e.evidence_span AS VARCHAR), ''))
                LIKE '%previous%'
                THEN 'historical'
            WHEN e.present_or_negated = 'present' THEN 'definite'
            ELSE 'uncertain'
        END AS rai_given_flag,

        ROW_NUMBER() OVER (
            PARTITION BY e.research_id
            ORDER BY TRY_CAST(COALESCE(e.entity_date, e.note_date) AS DATE) ASC NULLS LAST
        ) AS rai_episode_id

    FROM note_entities_medications e
    WHERE (
        LOWER(e.entity_value_norm) LIKE '%rai%'
        OR LOWER(e.entity_value_norm) LIKE '%radioactive%'
        OR LOWER(e.entity_value_norm) LIKE '%i-131%'
        OR LOWER(e.entity_value_norm) LIKE '%i131%'
        OR LOWER(e.entity_value_norm) LIKE '%iodine%'
        OR LOWER(e.entity_value_norm) LIKE '%thyrogen%'
    )

    UNION ALL

    SELECT
        e.research_id,
        e.entity_value_norm,
        e.entity_value_raw,
        e.present_or_negated,
        e.confidence,
        e.entity_date,
        e.note_date,
        TRY_CAST(COALESCE(e.entity_date, e.note_date) AS DATE) AS rai_date,
        CASE
            WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'entity_date'
            WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'note_date'
            ELSE 'unrecoverable'
        END AS rai_date_source,
        CASE
            WHEN TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'day'
            WHEN TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'day'
            ELSE NULL
        END AS rai_date_granularity,
        e.evidence_span AS rai_mention_text,
        'note_entities_procedures' AS source_table,
        'enriched_note_entities_procedures' AS source_view,
        CASE
            WHEN LOWER(e.entity_value_norm) LIKE '%ablation%' THEN 'ablation'
            WHEN LOWER(e.entity_value_raw) LIKE '%rai%' THEN 'RAI'
            WHEN LOWER(e.entity_value_raw) LIKE '%i-131%' THEN 'I-131'
            ELSE e.entity_value_norm
        END AS rai_term_normalized,
        CASE
            WHEN e.present_or_negated = 'negated' THEN 'negated'
            WHEN LOWER(COALESCE(CAST(e.evidence_span AS VARCHAR), ''))
                LIKE '%will receive%'
                OR LOWER(COALESCE(CAST(e.evidence_span AS VARCHAR), ''))
                LIKE '%plan%'
                THEN 'planned'
            WHEN LOWER(COALESCE(CAST(e.evidence_span AS VARCHAR), ''))
                LIKE '%history%'
                OR LOWER(COALESCE(CAST(e.evidence_span AS VARCHAR), ''))
                LIKE '%prior%'
                THEN 'historical'
            WHEN e.present_or_negated = 'present' THEN 'definite'
            ELSE 'uncertain'
        END AS rai_given_flag,
        ROW_NUMBER() OVER (
            PARTITION BY e.research_id
            ORDER BY TRY_CAST(COALESCE(e.entity_date, e.note_date) AS DATE)
                     ASC NULLS LAST
        ) AS rai_episode_id
    FROM note_entities_procedures e
    WHERE e.present_or_negated = 'present'
      AND (
          LOWER(e.entity_value_norm) LIKE '%ablation%'
          OR LOWER(e.entity_value_raw) LIKE '%rai%'
          OR LOWER(e.entity_value_raw) LIKE '%radioactive%'
          OR LOWER(e.entity_value_raw) LIKE '%i-131%'
      )
),
-- All surgeries for nearest-match
all_surgeries AS (
    SELECT
        research_id,
        TRY_CAST(surg_date AS DATE) AS surg_date,
        tumor_1_histologic_type,
        tumor_1_pt AS linked_stage,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST
        ) AS op_seq
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
),
-- Link RAI to nearest PRIOR surgery (bounded: surgery must be before RAI,
-- and within 730 days)
rai_surg_link AS (
    SELECT
        r.research_id,
        r.rai_episode_id,
        s.surg_date AS linked_surgery_date,
        s.linked_stage,
        s.tumor_1_histologic_type AS linked_histology,
        DATE_DIFF('day', s.surg_date, r.rai_date) AS days_surgery_to_rai,
        ROW_NUMBER() OVER (
            PARTITION BY r.research_id, r.rai_episode_id
            ORDER BY ABS(DATE_DIFF('day', s.surg_date,
                     COALESCE(r.rai_date, DATE '2099-01-01')))
        ) AS surg_rank
    FROM rai_mentions r
    LEFT JOIN all_surgeries s
        ON CAST(r.research_id AS BIGINT) = s.research_id
    WHERE r.rai_date IS NOT NULL AND s.surg_date IS NOT NULL
)
SELECT
    r.research_id,
    r.rai_episode_id,
    r.rai_mention_text,
    r.rai_term_normalized,
    r.rai_given_flag,
    r.rai_date,
    r.rai_date_source,
    r.rai_date_granularity,
    r.entity_value_raw AS dose_raw,
    -- Dose extraction: look for mCi pattern in evidence_span
    TRY_CAST(
        regexp_extract(COALESCE(CAST(r.rai_mention_text AS VARCHAR), ''),
                       '(\\d+\\.?\\d*)\\s*m[Cc]i', 1)
        AS DOUBLE
    ) AS dose_mci,
    r.entity_value_norm AS indication_raw,
    CASE
        WHEN sl.linked_surgery_date IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS post_thyroidectomy_flag,
    sl.linked_surgery_date,
    sl.days_surgery_to_rai,
    sl.linked_histology,
    sl.linked_stage,
    r.source_table,
    r.source_view,
    r.confidence,
    CASE
        WHEN r.rai_date IS NULL THEN TRUE
        WHEN sl.linked_surgery_date IS NULL THEN TRUE
        WHEN sl.days_surgery_to_rai < -30 THEN TRUE
        WHEN sl.days_surgery_to_rai > 730 THEN TRUE
        ELSE FALSE
    END AS unresolved_flag
FROM rai_mentions r
LEFT JOIN rai_surg_link sl
    ON CAST(r.research_id AS BIGINT) = CAST(sl.research_id AS BIGINT)
    AND r.rai_episode_id = sl.rai_episode_id
    AND sl.surg_rank = 1;
"""

RAI_UNRESOLVED_SQL = """
-- Phase C: RAI Unresolved Audit
-- Rows with no resolved_date or no plausible surgery anchor.
CREATE OR REPLACE VIEW rai_unresolved_audit_mv AS
SELECT
    r.research_id,
    r.rai_episode_id,
    r.rai_term_normalized,
    r.rai_given_flag,
    r.rai_date,
    r.rai_date_source,
    r.linked_surgery_date,
    r.days_surgery_to_rai,
    r.confidence,
    CASE
        WHEN r.rai_date IS NULL THEN 'no_rai_date'
        WHEN r.linked_surgery_date IS NULL THEN 'no_surgery_anchor'
        WHEN r.days_surgery_to_rai < -30 THEN 'implausible_pre_surgical'
        WHEN r.days_surgery_to_rai > 730 THEN 'extreme_interval'
        ELSE 'other'
    END AS unresolved_reason,
    r.source_table
FROM rai_episode_v2 r
WHERE r.unresolved_flag = TRUE;
"""


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE D — Timeline Rescue Layer
# ═══════════════════════════════════════════════════════════════════════════

TIMELINE_RESCUE_SQL = """
-- Phase D: Timeline Rescue Layer
-- Attempts conservative date rescue for medications and problem_list
-- rows that have no date anchors from Phase 1. Only assigns dates when
-- provenance is explicit. Classifies unresolvable rows by reason.
CREATE OR REPLACE VIEW timeline_rescue_mv AS
WITH unresolved_meds AS (
    SELECT
        'medications' AS source_table,
        e.research_id,
        e.entity_value_norm,
        e.entity_value_raw,
        e.entity_date,
        e.note_date,
        e.inferred_event_date,
        e.date_source AS original_date_source
    FROM enriched_note_entities_medications e
    WHERE e.date_source = 'unrecoverable'
),
unresolved_pl AS (
    SELECT
        'problem_list' AS source_table,
        e.research_id,
        e.entity_value_norm,
        e.entity_value_raw,
        e.entity_date,
        e.note_date,
        e.inferred_event_date,
        e.date_source AS original_date_source
    FROM enriched_note_entities_problem_list e
    WHERE e.date_source = 'unrecoverable'
),
unresolved_all AS (
    SELECT * FROM unresolved_meds
    UNION ALL
    SELECT * FROM unresolved_pl
),
-- Try rescue from path_synoptics primary surgery date
ps_anchor AS (
    SELECT
        research_id,
        TRY_CAST(surg_date AS DATE) AS surg_date,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST
        ) AS op_seq
    FROM path_synoptics
    WHERE surg_date IS NOT NULL AND surg_date != ''
),
-- Try rescue from FNA date
fna_anchor AS (
    SELECT
        research_id,
        TRY_CAST(fna_date_parsed AS DATE) AS fna_date,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY TRY_CAST(fna_date_parsed AS DATE) DESC NULLS LAST
        ) AS fna_seq
    FROM fna_history
    WHERE fna_date_parsed IS NOT NULL AND fna_date_parsed != ''
)
SELECT
    u.source_table,
    u.research_id,
    u.entity_value_norm,
    u.entity_value_raw,
    u.entity_date AS original_entity_date,
    u.note_date AS original_note_date,
    COALESCE(ps.surg_date, fna.fna_date) AS inferred_event_date,
    CASE
        WHEN ps.surg_date IS NOT NULL THEN 'day'
        WHEN fna.fna_date IS NOT NULL THEN 'day'
        ELSE NULL
    END AS date_granularity,
    CASE
        WHEN ps.surg_date IS NOT NULL THEN 'path_synoptics_primary_surgery'
        WHEN fna.fna_date IS NOT NULL THEN 'fna_history_latest'
        ELSE 'unrecoverable'
    END AS date_source,
    CASE
        WHEN ps.surg_date IS NOT NULL THEN 40
        WHEN fna.fna_date IS NOT NULL THEN 35
        ELSE 0
    END AS date_confidence,
    -- Unresolvable classification
    CASE
        WHEN ps.surg_date IS NULL AND fna.fna_date IS NULL
             AND NOT EXISTS (
                SELECT 1 FROM path_synoptics p2
                WHERE p2.research_id = CAST(u.research_id AS BIGINT)
             )
            THEN 'patient_missing_anchor_tables'
        WHEN u.entity_date IS NOT NULL
             AND TRY_CAST(u.entity_date AS DATE) IS NULL
            THEN 'parse_failure'
        WHEN ps.surg_date IS NULL AND fna.fna_date IS NULL
            THEN 'no_source_date'
        ELSE 'rescued'
    END AS rescue_status
FROM unresolved_all u
LEFT JOIN ps_anchor ps
    ON CAST(u.research_id AS BIGINT) = ps.research_id AND ps.op_seq = 1
LEFT JOIN fna_anchor fna
    ON CAST(u.research_id AS BIGINT) = fna.research_id AND fna.fna_seq = 1;
"""

TIMELINE_UNRESOLVED_SUMMARY_SQL = """
-- Phase D: Timeline Unresolved Summary
-- Remaining unresolved timeline burden by table and reason.
CREATE OR REPLACE VIEW timeline_unresolved_summary_mv AS
SELECT
    source_table,
    rescue_status,
    COUNT(*) AS row_count,
    COUNT(DISTINCT research_id) AS patient_count,
    ROUND(100.0 * COUNT(*) /
        SUM(COUNT(*)) OVER (PARTITION BY source_table), 2) AS pct_of_table
FROM timeline_rescue_mv
GROUP BY 1, 2
ORDER BY source_table, row_count DESC;
"""


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE E — Validation Framework V2
# ═══════════════════════════════════════════════════════════════════════════

VALIDATION_V2_SQL = """
-- Phase E: Validation Failures V2
-- Comprehensive promotion-grade QA layer across all reconciliation domains.
-- Each row is a specific validation issue with severity and recommended action.
CREATE OR REPLACE VIEW validation_failures_v2 AS

-- Domain 1: Date validation from enriched entities
WITH date_issues AS (
    SELECT
        domain,
        research_id,
        'date_validation' AS validation_domain,
        CASE
            WHEN inferred_event_date IS NULL THEN 'no_date_recoverable'
            WHEN inferred_event_date > CURRENT_DATE THEN 'future_date'
            WHEN inferred_event_date < DATE '1990-01-01' THEN 'implausible_historical_date'
            WHEN entity_date IS NOT NULL AND note_date IS NOT NULL
                 AND ABS(DATE_DIFF('day',
                         TRY_CAST(entity_date AS DATE),
                         TRY_CAST(note_date AS DATE))) > 365
                THEN 'entity_vs_note_date_gap'
            WHEN date_confidence < 50 THEN 'low_confidence'
        END AS issue_code,
        CASE
            WHEN inferred_event_date IS NULL THEN 'error'
            WHEN inferred_event_date > CURRENT_DATE THEN 'error'
            WHEN inferred_event_date < DATE '1990-01-01' THEN 'error'
            WHEN entity_date IS NOT NULL AND note_date IS NOT NULL
                 AND ABS(DATE_DIFF('day',
                         TRY_CAST(entity_date AS DATE),
                         TRY_CAST(note_date AS DATE))) > 365
                THEN 'warning'
            WHEN date_confidence < 50 THEN 'info'
            ELSE NULL
        END AS severity,
        CAST(inferred_event_date AS VARCHAR) AS detected_value,
        domain AS source_objects
    FROM (
        SELECT 'genetics' AS domain, research_id, entity_date, note_date,
               inferred_event_date, date_source, date_confidence
        FROM enriched_note_entities_genetics
        UNION ALL
        SELECT 'staging', research_id, entity_date, note_date,
               inferred_event_date, date_source, date_confidence
        FROM enriched_note_entities_staging
        UNION ALL
        SELECT 'procedures', research_id, entity_date, note_date,
               inferred_event_date, date_source, date_confidence
        FROM enriched_note_entities_procedures
        UNION ALL
        SELECT 'complications', research_id, entity_date, note_date,
               inferred_event_date, date_source, date_confidence
        FROM enriched_note_entities_complications
        UNION ALL
        SELECT 'medications', research_id, entity_date, note_date,
               inferred_event_date, date_source, date_confidence
        FROM enriched_note_entities_medications
        UNION ALL
        SELECT 'problem_list', research_id, entity_date, note_date,
               inferred_event_date, date_source, date_confidence
        FROM enriched_note_entities_problem_list
    ) all_enriched
    WHERE inferred_event_date IS NULL
       OR inferred_event_date > CURRENT_DATE
       OR inferred_event_date < DATE '1990-01-01'
       OR (entity_date IS NOT NULL AND note_date IS NOT NULL
           AND ABS(DATE_DIFF('day',
                   TRY_CAST(entity_date AS DATE),
                   TRY_CAST(note_date AS DATE))) > 365)
       OR date_confidence < 50
),

-- Domain 2: Molecular without anchor
molecular_issues AS (
    SELECT
        'molecular' AS domain,
        CAST(research_id AS VARCHAR) AS research_id,
        'molecular_linkage' AS validation_domain,
        'molecular_without_anchor' AS issue_code,
        'warning' AS severity,
        CAST(molecular_date AS VARCHAR) AS detected_value,
        'molecular_episode_v2' AS source_objects
    FROM molecular_episode_v2
    WHERE unresolved_flag = TRUE
),

-- Domain 3: RAI without surgery anchor
rai_issues AS (
    SELECT
        'rai' AS domain,
        CAST(research_id AS VARCHAR) AS research_id,
        'rai_linkage' AS validation_domain,
        CASE
            WHEN rai_date IS NULL THEN 'rai_without_date'
            WHEN linked_surgery_date IS NULL THEN 'rai_without_surgery_anchor'
            WHEN days_surgery_to_rai < -30 THEN 'invalid_interval_surgery_to_rai'
            WHEN days_surgery_to_rai > 730 THEN 'invalid_interval_surgery_to_rai'
        END AS issue_code,
        CASE
            WHEN rai_date IS NULL THEN 'error'
            WHEN linked_surgery_date IS NULL THEN 'warning'
            ELSE 'warning'
        END AS severity,
        CAST(days_surgery_to_rai AS VARCHAR) AS detected_value,
        'rai_episode_v2' AS source_objects
    FROM rai_episode_v2
    WHERE unresolved_flag = TRUE
),

-- Domain 4: Histology conflicts
histology_issues AS (
    SELECT
        'histology' AS domain,
        CAST(research_id AS VARCHAR) AS research_id,
        'histology_reconciliation' AS validation_domain,
        'histology_conflict' AS issue_code,
        'warning' AS severity,
        source_histology_raw_ps || ' vs ' ||
            COALESCE(source_histology_raw_tp, 'NULL') AS detected_value,
        'histology_reconciliation_v2' AS source_objects
    FROM histology_reconciliation_v2
    WHERE histology_discordance_flag = TRUE
),

-- Domain 5: Stage conflicts
stage_issues AS (
    SELECT
        'staging' AS domain,
        CAST(research_id AS VARCHAR) AS research_id,
        'stage_reconciliation' AS validation_domain,
        'stage_conflict' AS issue_code,
        'warning' AS severity,
        COALESCE(t_stage_source_path, 'NULL') || ' vs ' ||
            COALESCE(t_stage_source_note, 'NULL') AS detected_value,
        'histology_reconciliation_v2' AS source_objects
    FROM histology_reconciliation_v2
    WHERE stage_discordance_flag = TRUE
),

-- Domain 6: N-stage conflicts
n_stage_issues AS (
    SELECT
        'staging' AS domain,
        CAST(research_id AS VARCHAR) AS research_id,
        'stage_reconciliation' AS validation_domain,
        'n_stage_conflict' AS issue_code,
        'info' AS severity,
        COALESCE(n_stage_source_path, 'NULL') || ' vs ' ||
            COALESCE(n_stage_source_note, 'NULL') AS detected_value,
        'histology_reconciliation_v2' AS source_objects
    FROM histology_reconciliation_v2
    WHERE n_stage_discordance_flag = TRUE
)

SELECT
    ROW_NUMBER() OVER (ORDER BY severity DESC, domain, research_id) AS validation_id,
    CAST(research_id AS VARCHAR) AS research_id,
    domain,
    validation_domain,
    severity,
    issue_code,
    CASE
        WHEN issue_code = 'no_date_recoverable'
            THEN 'Entity has no recoverable date from any source'
        WHEN issue_code = 'future_date'
            THEN 'Inferred date is in the future'
        WHEN issue_code = 'implausible_historical_date'
            THEN 'Inferred date before 1990'
        WHEN issue_code = 'entity_vs_note_date_gap'
            THEN 'Entity date and note date differ by >1 year'
        WHEN issue_code = 'low_confidence'
            THEN 'Date confidence score below 50'
        WHEN issue_code = 'molecular_without_anchor'
            THEN 'Molecular test has no date or episode linkage'
        WHEN issue_code = 'rai_without_date'
            THEN 'RAI mention has no resolved date'
        WHEN issue_code = 'rai_without_surgery_anchor'
            THEN 'RAI mention has no linked surgery'
        WHEN issue_code = 'invalid_interval_surgery_to_rai'
            THEN 'Surgery-to-RAI interval implausible (<-30d or >730d)'
        WHEN issue_code = 'histology_conflict'
            THEN 'Histology type differs between path_synoptics and tumor_pathology'
        WHEN issue_code = 'stage_conflict'
            THEN 'T-stage differs between pathology report and clinical notes'
        WHEN issue_code = 'n_stage_conflict'
            THEN 'N-stage differs between pathology report and clinical notes'
        ELSE issue_code
    END AS issue_description,
    source_objects,
    NULL AS related_episode_ids,
    detected_value,
    CASE
        WHEN issue_code IN ('future_date','implausible_historical_date')
            THEN 'date must be between 1990-01-01 and today'
        WHEN issue_code = 'entity_vs_note_date_gap'
            THEN 'entity_date and note_date should be within 365 days'
        WHEN issue_code = 'invalid_interval_surgery_to_rai'
            THEN 'surgery_to_rai interval should be -30..730 days'
        ELSE NULL
    END AS expected_rule,
    CASE
        WHEN severity = 'error' THEN 'manual_review_required'
        WHEN severity = 'warning' THEN 'verify_and_resolve'
        WHEN severity = 'info' THEN 'monitor'
        ELSE NULL
    END AS recommended_action
FROM (
    SELECT domain, research_id, validation_domain, issue_code,
           severity, detected_value, source_objects
    FROM date_issues WHERE issue_code IS NOT NULL
    UNION ALL
    SELECT * FROM molecular_issues
    UNION ALL
    SELECT * FROM rai_issues WHERE issue_code IS NOT NULL
    UNION ALL
    SELECT * FROM histology_issues
    UNION ALL
    SELECT * FROM stage_issues
    UNION ALL
    SELECT * FROM n_stage_issues
) combined;
"""

PATIENT_ROLLUP_SQL = """
-- Phase E: Patient Validation Rollup
-- Highest-severity issue per patient across all domains.
CREATE OR REPLACE VIEW patient_validation_rollup_mv AS
SELECT
    research_id,
    COUNT(*) AS total_issues,
    SUM(CASE WHEN severity = 'error' THEN 1 ELSE 0 END) AS error_count,
    SUM(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) AS warning_count,
    SUM(CASE WHEN severity = 'info' THEN 1 ELSE 0 END) AS info_count,
    CASE
        WHEN SUM(CASE WHEN severity = 'error' THEN 1 ELSE 0 END) > 0
            THEN 'error'
        WHEN SUM(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) > 0
            THEN 'warning'
        ELSE 'info'
    END AS highest_severity,
    STRING_AGG(DISTINCT domain, ', ' ORDER BY domain) AS affected_domains,
    STRING_AGG(DISTINCT issue_code, ', ' ORDER BY issue_code) AS issue_codes
FROM validation_failures_v2
GROUP BY research_id;
"""


# ═══════════════════════════════════════════════════════════════════════════
#  PHASE F — Analytics Readiness Views
# ═══════════════════════════════════════════════════════════════════════════

PATIENT_TIMELINE_V2_SQL = """
-- Phase F: Patient Master Timeline V2
-- Clean performant view combining surgery, molecular, and RAI episodes
-- for Streamlit patient timeline tab.
CREATE OR REPLACE VIEW patient_master_timeline_v2 AS
WITH surgeries AS (
    SELECT
        research_id,
        op_seq AS episode_seq,
        pathology_date AS event_date,
        'surgery' AS event_type,
        COALESCE(source_histology_raw, 'unknown') AS event_detail,
        t_stage_reconciled AS stage_info,
        CASE WHEN unresolved_flag THEN 'unresolved' ELSE 'resolved' END AS status
    FROM histology_reconciliation_v2
),
molecular AS (
    SELECT
        research_id,
        molecular_episode_id AS episode_seq,
        molecular_date AS event_date,
        'molecular_test' AS event_type,
        COALESCE(test_name_raw, platform_normalized, 'unknown') AS event_detail,
        result_category_normalized AS stage_info,
        CASE WHEN unresolved_flag THEN 'unresolved' ELSE 'resolved' END AS status
    FROM molecular_episode_v2
),
rai AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        rai_episode_id AS episode_seq,
        rai_date AS event_date,
        'rai_treatment' AS event_type,
        rai_term_normalized AS event_detail,
        rai_given_flag AS stage_info,
        CASE WHEN unresolved_flag THEN 'unresolved' ELSE 'resolved' END AS status
    FROM rai_episode_v2
)
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    episode_seq,
    event_date,
    event_type,
    event_detail,
    stage_info,
    status
FROM (
    SELECT * FROM surgeries
    UNION ALL
    SELECT * FROM molecular
    UNION ALL
    SELECT * FROM rai
) all_events
ORDER BY research_id, event_date NULLS LAST, event_type;
"""

RECONCILIATION_SUMMARY_SQL = """
-- Phase F: Patient Reconciliation Summary
-- One row per patient summarizing reconciliation status across domains.
CREATE OR REPLACE VIEW patient_reconciliation_summary_v AS
SELECT
    CAST(mc.research_id AS BIGINT) AS research_id,
    mc.age_at_surgery,
    mc.sex,
    mc.surgery_date,

    -- Histology summary
    h.histology_normalized,
    h.variant_normalized,
    h.t_stage_reconciled,
    h.reconciliation_status AS histology_status,
    h.histology_discordance_flag,
    h.stage_discordance_flag,

    -- Molecular summary (count of tests, any high-risk)
    mol.molecular_test_count,
    mol.has_high_risk_molecular,

    -- RAI summary
    rai_s.rai_episode_count,
    rai_s.has_definite_rai,

    -- Validation summary
    val.total_issues,
    val.highest_severity

FROM master_cohort mc

LEFT JOIN (
    SELECT research_id, histology_normalized, variant_normalized,
           t_stage_reconciled, reconciliation_status,
           histology_discordance_flag, stage_discordance_flag
    FROM histology_reconciliation_v2
    WHERE op_seq = 1
) h ON CAST(mc.research_id AS BIGINT) = CAST(h.research_id AS BIGINT)

LEFT JOIN (
    SELECT
        research_id,
        COUNT(*) AS molecular_test_count,
        MAX(CASE WHEN high_risk_molecular_flag THEN 1 ELSE 0 END) > 0
            AS has_high_risk_molecular
    FROM molecular_episode_v2
    GROUP BY research_id
) mol ON CAST(mc.research_id AS BIGINT) = CAST(mol.research_id AS BIGINT)

LEFT JOIN (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        COUNT(*) AS rai_episode_count,
        MAX(CASE WHEN rai_given_flag = 'definite' THEN 1 ELSE 0 END) > 0
            AS has_definite_rai
    FROM rai_episode_v2
    GROUP BY CAST(research_id AS BIGINT)
) rai_s ON CAST(mc.research_id AS BIGINT) = rai_s.research_id

LEFT JOIN patient_validation_rollup_mv val
    ON CAST(mc.research_id AS VARCHAR) = val.research_id;
"""

EPISODE_AUDIT_SQL = """
-- Phase F: Patient Episode Audit
-- Per-patient audit surface showing all episodes with their resolution status.
CREATE OR REPLACE VIEW patient_episode_audit_v AS
SELECT
    CAST(research_id AS BIGINT) AS research_id,
    event_type AS episode_type,
    episode_seq,
    event_date,
    event_detail,
    stage_info,
    status AS resolution_status
FROM patient_master_timeline_v2
ORDER BY research_id, event_date NULLS LAST;
"""


# ═══════════════════════════════════════════════════════════════════════════
#  Deployment & Profiling
# ═══════════════════════════════════════════════════════════════════════════


def deploy_view(
    con: duckdb.DuckDBPyConnection,
    name: str,
    sql: str,
    view_log: list[tuple[str, str]],
) -> bool:
    try:
        con.execute(sql)
        cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"  Created {name:<50} {cnt:>8,} rows")
        view_log.append((name, sql))
        return True
    except Exception as e:
        print(f"  FAILED  {name}: {e}")
        return False


def profile_before(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Capture baseline unresolved counts before Phase 2 upgrades."""
    section("BASELINE COUNTS (BEFORE)")
    counts: dict[str, int] = {}

    queries = {
        "histology_unresolved": """
            SELECT COUNT(*)
            FROM histology_reconciliation_mv
            WHERE reconciliation_status = 'path_missing'
               OR (t_stage_discordant = TRUE)
        """,
        "molecular_unresolved": """
            SELECT COUNT(*)
            FROM molecular_episode_mv
            WHERE episode_date_source = 'unresolved'
        """,
        "rai_unresolved": """
            SELECT COUNT(*)
            FROM rai_episode_mv
            WHERE rai_resolved_date IS NULL
        """,
        "timeline_unresolved": """
            SELECT COUNT(*)
            FROM missing_date_associations_audit
            WHERE date_source = 'unrecoverable'
        """,
        "validation_failures": """
            SELECT COUNT(*) FROM validation_failures_mv
        """,
    }
    for label, sql in queries.items():
        try:
            cnt = con.execute(sql).fetchone()[0]
            counts[label] = cnt
            print(f"  {label:<40} {cnt:>8,}")
        except Exception as e:
            counts[label] = -1
            print(f"  {label:<40} ERROR: {e}")

    return counts


def profile_after(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Capture post-upgrade counts."""
    section("UPGRADED COUNTS (AFTER)")
    counts: dict[str, int] = {}

    queries = {
        "histology_unresolved_v2": """
            SELECT COUNT(*)
            FROM histology_reconciliation_v2
            WHERE unresolved_flag = TRUE
        """,
        "histology_discordant_v2": """
            SELECT COUNT(*)
            FROM histology_reconciliation_v2
            WHERE histology_discordance_flag = TRUE
               OR stage_discordance_flag = TRUE
        """,
        "molecular_unresolved_v2": """
            SELECT COUNT(*)
            FROM molecular_episode_v2
            WHERE unresolved_flag = TRUE
        """,
        "molecular_low_confidence_v2": """
            SELECT COUNT(*)
            FROM molecular_episode_v2
            WHERE linkage_confidence IN ('none', 'low')
        """,
        "rai_unresolved_v2": """
            SELECT COUNT(*)
            FROM rai_episode_v2
            WHERE unresolved_flag = TRUE
        """,
        "timeline_still_unresolved": """
            SELECT COUNT(*)
            FROM timeline_rescue_mv
            WHERE rescue_status != 'rescued'
        """,
        "timeline_rescued": """
            SELECT COUNT(*)
            FROM timeline_rescue_mv
            WHERE rescue_status = 'rescued'
        """,
        "validation_v2_total": """
            SELECT COUNT(*) FROM validation_failures_v2
        """,
        "validation_v2_errors": """
            SELECT COUNT(*) FROM validation_failures_v2 WHERE severity = 'error'
        """,
        "validation_v2_warnings": """
            SELECT COUNT(*) FROM validation_failures_v2 WHERE severity = 'warning'
        """,
        "validation_v2_info": """
            SELECT COUNT(*) FROM validation_failures_v2 WHERE severity = 'info'
        """,
        "patients_with_issues": """
            SELECT COUNT(*) FROM patient_validation_rollup_mv
        """,
    }
    for label, sql in queries.items():
        try:
            cnt = con.execute(sql).fetchone()[0]
            counts[label] = cnt
            print(f"  {label:<40} {cnt:>8,}")
        except Exception as e:
            counts[label] = -1
            print(f"  {label:<40} ERROR: {e}")

    return counts


def write_sql_file(view_log: list[tuple[str, str]]) -> None:
    """Write generated SQL definitions to file."""
    with open(SQL_OUT, "w") as f:
        f.write("-- Reconciliation V2 Views\n")
        f.write("-- Generated by 16_reconciliation_v2.py\n")
        f.write("-- Deploy to thyroid_research_2026 via: USE thyroid_research_2026;\n")
        f.write("--\n")
        f.write("-- Phases: A (histology), B (molecular), C (RAI),\n")
        f.write("--         D (timeline rescue), E (validation), F (analytics)\n")
        f.write("--\n")
        f.write("-- Depends on: 15_date_association_views.sql (enriched_* views)\n\n")
        for name, sql in view_log:
            f.write(f"-- === {name} ===\n")
            f.write(sql.strip())
            f.write("\n\n")
    print(f"  SQL definitions saved to: {SQL_OUT}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconciliation V2 — Episode Framework"
    )
    parser.add_argument(
        "--md", action="store_true",
        help="Use MotherDuck instead of local DuckDB",
    )
    args = parser.parse_args()

    print("=" * 80)
    print("  RECONCILIATION V2 — Episode Framework")
    print("  Mode: " + ("MotherDuck" if args.md else "Local DuckDB"))
    print("=" * 80)

    if args.md:
        from motherduck_client import MotherDuckClient
        client = MotherDuckClient()
        con = client.connect_rw()
    else:
        con = duckdb.connect(str(DB_PATH))

    # ── Register data ──
    section("REGISTERING DATA SOURCES")
    if args.md:
        available: list[str] = []
        for tbl in REQUIRED_TABLES + OPTIONAL_TABLES:
            if table_available(con, tbl):
                available.append(tbl)
                print(f"  Available {tbl}")
            else:
                print(f"  MISSING   {tbl}")
    else:
        available = register_parquets(con)

    # Create stubs for DVC-tracked tables not in local DuckDB
    # These are only needed locally; MotherDuck has the real tables
    if not args.md:
        stubs = {
            "tumor_pathology": """
                CREATE TABLE IF NOT EXISTS tumor_pathology AS
                SELECT NULL::VARCHAR AS research_id,
                       NULL::VARCHAR AS histology_1_type,
                       NULL::VARCHAR AS tumor_1_histology_variant,
                       NULL::VARCHAR AS histology_1_t_stage_ajcc8,
                       NULL::VARCHAR AS histology_1_n_stage_ajcc8,
                       NULL::VARCHAR AS histology_1_m_stage_ajcc8,
                       NULL::VARCHAR AS histology_1_overall_stage_ajcc8,
                       NULL::VARCHAR AS histology_1_largest_tumor_cm,
                       NULL::VARCHAR AS tumor_focality_overall,
                       NULL::VARCHAR AS braf_mutation_mentioned,
                       NULL::VARCHAR AS ras_mutation_mentioned,
                       NULL::VARCHAR AS ret_mutation_mentioned,
                       NULL::VARCHAR AS tert_mutation_mentioned
                WHERE FALSE
            """,
            "master_cohort": """
                CREATE TABLE IF NOT EXISTS master_cohort AS
                SELECT NULL::VARCHAR AS research_id,
                       NULL::VARCHAR AS age_at_surgery,
                       NULL::VARCHAR AS sex,
                       NULL::VARCHAR AS surgery_date
                WHERE FALSE
            """,
            "benign_pathology": """
                CREATE TABLE IF NOT EXISTS benign_pathology AS
                SELECT NULL::VARCHAR AS research_id
                WHERE FALSE
            """,
        }
        for tbl, stub_sql in stubs.items():
            if not table_available(con, tbl):
                con.execute(stub_sql)
                print(f"  STUB created for {tbl} (DVC-tracked, not local)")

    # Check Phase 1 views exist (from 15_date_association_audit.py)
    phase1_views = [
        "enriched_note_entities_genetics",
        "enriched_note_entities_staging",
        "enriched_note_entities_procedures",
        "enriched_note_entities_complications",
        "enriched_note_entities_medications",
        "enriched_note_entities_problem_list",
        "missing_date_associations_audit",
        "histology_reconciliation_mv",
        "molecular_episode_mv",
        "rai_episode_mv",
        "validation_failures_mv",
    ]
    section("CHECKING PHASE 1 PREREQUISITES")
    missing_prereqs = []
    for v in phase1_views:
        if table_available(con, v):
            print(f"  OK   {v}")
        else:
            print(f"  MISS {v}")
            missing_prereqs.append(v)

    if missing_prereqs:
        print("\n  WARNING: Missing Phase 1 views. Run 15_date_association_audit.py "
              "first if deploying to a fresh database.")
        print("  Will attempt to create Phase 2 views anyway (some may fail).\n")

    # ── Discover molecular date column ──
    genetics_date_col = discover_genetics_date_col(con)
    if genetics_date_col:
        print(f"  Molecular date column: '{genetics_date_col}'")
    else:
        print("  Molecular date column: NONE (will use NULL)")

    # ── Baseline profiling ──
    before = profile_before(con)

    # ── Deploy Phase 2 views ──
    view_log: list[tuple[str, str]] = []

    # Phase A
    section("PHASE A: HISTOLOGY RECONCILIATION V2")
    deploy_view(con, "histology_reconciliation_v2",
                HISTOLOGY_RECON_V2_SQL, view_log)

    # Phase B
    section("PHASE B: MOLECULAR EPISODE V2")
    mol_sql = build_molecular_v2_sql(genetics_date_col)
    deploy_view(con, "molecular_episode_v2", mol_sql, view_log)
    deploy_view(con, "molecular_unresolved_audit_mv",
                MOLECULAR_UNRESOLVED_SQL, view_log)

    # Phase C
    section("PHASE C: RAI EPISODE V2")
    deploy_view(con, "rai_episode_v2", RAI_EPISODE_V2_SQL, view_log)
    deploy_view(con, "rai_unresolved_audit_mv",
                RAI_UNRESOLVED_SQL, view_log)

    # Phase D
    section("PHASE D: TIMELINE RESCUE")
    deploy_view(con, "timeline_rescue_mv",
                TIMELINE_RESCUE_SQL, view_log)
    deploy_view(con, "timeline_unresolved_summary_mv",
                TIMELINE_UNRESOLVED_SUMMARY_SQL, view_log)

    # Phase E
    section("PHASE E: VALIDATION FRAMEWORK V2")
    deploy_view(con, "validation_failures_v2",
                VALIDATION_V2_SQL, view_log)
    deploy_view(con, "patient_validation_rollup_mv",
                PATIENT_ROLLUP_SQL, view_log)

    # Phase F
    section("PHASE F: ANALYTICS READINESS VIEWS")
    deploy_view(con, "patient_master_timeline_v2",
                PATIENT_TIMELINE_V2_SQL, view_log)
    deploy_view(con, "patient_reconciliation_summary_v",
                RECONCILIATION_SUMMARY_SQL, view_log)
    deploy_view(con, "patient_episode_audit_v",
                EPISODE_AUDIT_SQL, view_log)

    # ── Post-deployment profiling ──
    after = profile_after(con)

    # ── Before/After comparison ──
    section("BEFORE / AFTER COMPARISON")
    comparisons = [
        ("Histology unresolved",
         before.get("histology_unresolved", -1),
         after.get("histology_unresolved_v2", -1)),
        ("Molecular unresolved",
         before.get("molecular_unresolved", -1),
         after.get("molecular_unresolved_v2", -1)),
        ("RAI unresolved",
         before.get("rai_unresolved", -1),
         after.get("rai_unresolved_v2", -1)),
        ("Timeline unresolved",
         before.get("timeline_unresolved", -1),
         after.get("timeline_still_unresolved", -1)),
        ("Validation failures (total)",
         before.get("validation_failures", -1),
         after.get("validation_v2_total", -1)),
    ]
    print(f"  {'metric':<35} {'before':>10} {'after':>10} {'delta':>10}")
    print("  " + "-" * 67)
    for label, b, a in comparisons:
        delta = a - b if b >= 0 and a >= 0 else "N/A"
        print(f"  {label:<35} {b:>10} {a:>10} {str(delta):>10}")

    # ── Validation severity breakdown ──
    section("VALIDATION SEVERITY BREAKDOWN")
    try:
        rows = con.execute("""
            SELECT severity, domain, issue_code, COUNT(*) AS n,
                   COUNT(DISTINCT research_id) AS patients
            FROM validation_failures_v2
            GROUP BY 1, 2, 3
            ORDER BY
                CASE severity WHEN 'error' THEN 1
                              WHEN 'warning' THEN 2
                              ELSE 3 END,
                n DESC
        """).fetchall()
        print(f"  {'severity':<10} {'domain':<15} {'issue_code':<35} "
              f"{'rows':>7} {'patients':>9}")
        print("  " + "-" * 78)
        for r in rows:
            print(f"  {str(r[0]):<10} {str(r[1]):<15} {str(r[2]):<35} "
                  f"{r[3]:>7} {r[4]:>9}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # ── Write SQL file ──
    section("WRITING SQL FILE")
    write_sql_file(view_log)

    # ── Remaining gaps ──
    section("REMAINING KNOWN GAPS")
    gap_queries = {
        "Histology: patients with no path_synoptics": """
            SELECT COUNT(DISTINCT CAST(research_id AS BIGINT))
            FROM tumor_pathology
            WHERE CAST(research_id AS BIGINT) NOT IN (
                SELECT CAST(research_id AS BIGINT) FROM path_synoptics
            )
        """,
        "Molecular: unresolved episodes": """
            SELECT COUNT(*) FROM molecular_unresolved_audit_mv
        """,
        "RAI: unresolved episodes": """
            SELECT COUNT(*) FROM rai_unresolved_audit_mv
        """,
        "Timeline: still unresolvable": """
            SELECT COUNT(*) FROM timeline_rescue_mv
            WHERE rescue_status != 'rescued'
        """,
        "Validation: error-severity issues": """
            SELECT COUNT(*) FROM validation_failures_v2
            WHERE severity = 'error'
        """,
    }
    for label, sql in gap_queries.items():
        try:
            cnt = con.execute(sql).fetchone()[0]
            print(f"  {label:<50} {cnt:>8,}")
        except Exception as e:
            print(f"  {label:<50} ERROR: {e}")

    # ── Files changed ──
    section("FILES CREATED/MODIFIED")
    print(f"  scripts/16_reconciliation_v2.py       (this script)")
    print(f"  scripts/16_reconciliation_v2_views.sql (generated SQL)")

    con.close()
    print(f"\n{'=' * 80}")
    print("  DONE — Reconciliation V2 complete")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
