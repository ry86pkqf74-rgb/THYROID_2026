#!/usr/bin/env python3
"""
39_gap_remediation.py — Address remaining gaps + note-body date recovery

Gap 0 — Note-body date recovery (all domains):
  - 24,572 entity rows have NULL entity_date AND NULL note_date.
  - clinical_notes_long.note_date is also NULL for all of them.
  - BUT note TEXT body contains parseable dates (MM/DD/YYYY, MM/DD/YY).
  - Skip DOB by extracting after clinical content markers (HPI, Chief Complaint, etc.).
  - Recovers ~16,644 entity rows at day precision, confidence 50.
  - New fallback tier: entity_date → note_date → note_body_date → surgery/FNA.

Gap 1 — Molecular (9,216 undated):
  - 8,802 placeholder stubs flagged and excluded.
  - Single-FNA patient confidence boost.

Gap 2 — RAI (585 no-date):
  - Note-text date extraction from clinical_notes_long body.

Gap 3 — Histology (6,386 no staging):
  - AJCC 8th Ed T/N staging calculated from tumor size + ETE + LN.
  - Tiered eligibility.

Updated views:
  - note_body_date_recovery_v (new helper: extracts dates from note text body)
  - enriched_note_entities_* (all 6: adds note_body_date fallback)
  - molecular_episode_v3, rai_episode_v3, histology_analysis_cohort_v

Run after scripts 15, 16, 17, 18.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "processed"
DB_PATH = ROOT / "thyroid_master.duckdb"

sys.path.insert(0, str(ROOT))


def section(title: str) -> None:
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}\n")


def table_available(con: duckdb.DuckDBPyConnection, tbl: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
        return True
    except Exception:
        return False


def register_parquets(con: duckdb.DuckDBPyConnection) -> None:
    for tbl in ["path_synoptics", "molecular_testing", "fna_history",
                "note_entities_staging", "note_entities_genetics",
                "note_entities_medications", "note_entities_procedures",
                "note_entities_complications", "note_entities_problem_list",
                "clinical_notes_long"]:
        pq = PROCESSED / f"{tbl}.parquet"
        if pq.exists():
            con.execute(
                f"CREATE OR REPLACE TABLE {tbl} AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )
            cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            print(f"  Registered {tbl:<45} {cnt:>8,} rows")
        elif table_available(con, tbl):
            print(f"  Existing  {tbl:<45}")
        else:
            print(f"  SKIP      {tbl:<45}")

    if not table_available(con, "tumor_pathology"):
        con.execute("""
            CREATE TABLE IF NOT EXISTS tumor_pathology AS
            SELECT NULL::VARCHAR AS research_id WHERE FALSE
        """)


def deploy_view(con: duckdb.DuckDBPyConnection, name: str, sql: str) -> bool:
    try:
        con.execute(sql)
        cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"  {name:<55} {cnt:>8,} rows")
        return True
    except Exception as e:
        print(f"  FAILED  {name}: {e}")
        return False


# ═══════════════════════════════════════════════════════════════════════════════
#  GAP 0 — Note-body date recovery (all domains)
# ═══════════════════════════════════════════════════════════════════════════════

NOTE_BODY_DATE_RECOVERY_SQL = r"""
CREATE OR REPLACE VIEW note_body_date_recovery_v AS
WITH raw_extract AS (
    SELECT
        cn.note_row_id,
        cn.research_id,
        cn.note_type,
        regexp_extract(
            SUBSTRING(CAST(cn.note_text AS VARCHAR) FROM
                GREATEST(
                    CASE WHEN CAST(cn.note_text AS VARCHAR) LIKE '%History of Present Illness%'
                         THEN position('History of Present Illness' IN CAST(cn.note_text AS VARCHAR)) ELSE 0 END,
                    CASE WHEN CAST(cn.note_text AS VARCHAR) LIKE '%Chief Complaint%'
                         THEN position('Chief Complaint' IN CAST(cn.note_text AS VARCHAR)) ELSE 0 END,
                    CASE WHEN CAST(cn.note_text AS VARCHAR) LIKE '%Subjective%'
                         THEN position('Subjective' IN CAST(cn.note_text AS VARCHAR)) ELSE 0 END,
                    CASE WHEN CAST(cn.note_text AS VARCHAR) LIKE '%Assessment and Plan%'
                         THEN position('Assessment and Plan' IN CAST(cn.note_text AS VARCHAR)) ELSE 0 END,
                    CASE WHEN CAST(cn.note_text AS VARCHAR) LIKE '%PREOPERATIVE DIAGNOSIS%'
                         THEN position('PREOPERATIVE DIAGNOSIS' IN CAST(cn.note_text AS VARCHAR)) ELSE 0 END,
                    CASE WHEN CAST(cn.note_text AS VARCHAR) LIKE '%Date of admission%'
                         THEN position('Date of admission' IN CAST(cn.note_text AS VARCHAR)) ELSE 0 END,
                    CASE WHEN CAST(cn.note_text AS VARCHAR) LIKE '%Discharge%'
                         THEN position('Discharge' IN CAST(cn.note_text AS VARCHAR)) ELSE 0 END,
                    200
                )
            ),
            '\b(\d{1,2}/\d{1,2}/\d{2,4})\b', 1
        ) AS raw_date_str
    FROM clinical_notes_long cn
    WHERE cn.note_date IS NULL OR TRY_CAST(cn.note_date AS DATE) IS NULL
),
year_fixed AS (
    SELECT
        note_row_id, research_id, note_type, raw_date_str,
        CASE
            WHEN raw_date_str IS NULL OR raw_date_str = '' THEN NULL
            WHEN LENGTH(regexp_extract(raw_date_str, '(\d+)$', 1)) = 2
                 AND CAST(regexp_extract(raw_date_str, '(\d+)$', 1) AS INTEGER) <= 30
                THEN regexp_replace(raw_date_str, '(\d+)$',
                     CAST(2000 + CAST(regexp_extract(raw_date_str, '(\d+)$', 1) AS INTEGER) AS VARCHAR))
            WHEN LENGTH(regexp_extract(raw_date_str, '(\d+)$', 1)) = 2
                THEN regexp_replace(raw_date_str, '(\d+)$',
                     CAST(1900 + CAST(regexp_extract(raw_date_str, '(\d+)$', 1) AS INTEGER) AS VARCHAR))
            ELSE raw_date_str
        END AS norm_date_str
    FROM raw_extract
)
SELECT
    note_row_id, research_id, note_type, raw_date_str, norm_date_str,
    CASE
        WHEN norm_date_str IS NOT NULL AND norm_date_str != ''
            THEN TRY_CAST(TRY_STRPTIME(norm_date_str, '%m/%d/%Y') AS DATE)
        ELSE NULL
    END AS resolved_note_body_date,
    CASE
        WHEN norm_date_str IS NOT NULL AND norm_date_str != ''
             AND TRY_CAST(TRY_STRPTIME(norm_date_str, '%m/%d/%Y') AS DATE) IS NOT NULL
             AND TRY_CAST(TRY_STRPTIME(norm_date_str, '%m/%d/%Y') AS DATE)
                 BETWEEN DATE '1995-01-01' AND CURRENT_DATE
            THEN TRUE
        ELSE FALSE
    END AS date_is_plausible
FROM year_fixed;
"""


def _enriched_with_note_body_view(entity_table: str, has_surg_fna: bool) -> str:
    """Generate enriched view with note-body date fallback.

    Fallback chain: entity_date → note_date → note_body_date → surgery/FNA → NULL
    """
    if has_surg_fna:
        anchor_ctes = """
ps_primary AS (
    SELECT research_id, TRY_CAST(surg_date AS DATE) AS surg_date_parsed,
           ROW_NUMBER() OVER (PARTITION BY research_id
                              ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST) AS op_seq
    FROM path_synoptics WHERE surg_date IS NOT NULL AND surg_date != ''
),
fna_primary AS (
    SELECT research_id, TRY_CAST(fna_date_parsed AS DATE) AS fna_date,
           ROW_NUMBER() OVER (PARTITION BY research_id
                              ORDER BY TRY_CAST(fna_date_parsed AS DATE) DESC NULLS LAST,
                                       fna_index DESC) AS fna_seq
    FROM fna_history WHERE fna_date_parsed IS NOT NULL AND fna_date_parsed != ''
),"""
        anchor_joins = """
LEFT JOIN ps_primary ps ON CAST(e.research_id AS BIGINT) = ps.research_id AND ps.op_seq = 1
LEFT JOIN fna_primary fna ON CAST(e.research_id AS BIGINT) = fna.research_id AND fna.fna_seq = 1"""
        surg_coalesce = "ps.surg_date_parsed, fna.fna_date"
        surg_source_cases = """
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'surg_date'
            WHEN fna.fna_date IS NOT NULL THEN 'fna_date_parsed'"""
        surg_gran_cases = """
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'day'
            WHEN fna.fna_date IS NOT NULL THEN 'day'"""
        surg_conf_cases = """
            WHEN ps.surg_date_parsed IS NOT NULL THEN 40
            WHEN fna.fna_date IS NOT NULL THEN 35"""
        surg_anchor_cases = """
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'surgical'
            WHEN fna.fna_date IS NOT NULL THEN 'cytology'"""
        surg_table_cases = f"""
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'path_synoptics'
            WHEN fna.fna_date IS NOT NULL THEN 'fna_history'"""
        surg_status_check = "OR ps.surg_date_parsed IS NOT NULL OR fna.fna_date IS NOT NULL"
        surg_review_check = """
        WHEN ps.surg_date_parsed IS NOT NULL
             AND e.entity_date IS NULL AND TRY_CAST(e.note_date AS DATE) IS NULL
             AND nb.resolved_note_body_date IS NULL THEN TRUE
        WHEN fna.fna_date IS NOT NULL
             AND e.entity_date IS NULL AND TRY_CAST(e.note_date AS DATE) IS NULL
             AND nb.resolved_note_body_date IS NULL AND ps.surg_date_parsed IS NULL THEN TRUE"""
    else:
        anchor_ctes = """
ps_primary AS (
    SELECT research_id, TRY_CAST(surg_date AS DATE) AS surg_date_parsed,
           ROW_NUMBER() OVER (PARTITION BY research_id
                              ORDER BY TRY_CAST(surg_date AS DATE) ASC NULLS LAST) AS op_seq
    FROM path_synoptics WHERE surg_date IS NOT NULL AND surg_date != ''
),"""
        anchor_joins = """
LEFT JOIN ps_primary ps ON CAST(e.research_id AS BIGINT) = ps.research_id AND ps.op_seq = 1"""
        surg_coalesce = "ps.surg_date_parsed"
        surg_source_cases = """
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'surg_date'"""
        surg_gran_cases = """
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'day'"""
        surg_conf_cases = """
            WHEN ps.surg_date_parsed IS NOT NULL THEN 60"""
        surg_anchor_cases = """
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'surgical'"""
        surg_table_cases = f"""
            WHEN ps.surg_date_parsed IS NOT NULL THEN 'path_synoptics'"""
        surg_status_check = "OR ps.surg_date_parsed IS NOT NULL"
        surg_review_check = """
        WHEN ps.surg_date_parsed IS NOT NULL
             AND e.entity_date IS NULL AND TRY_CAST(e.note_date AS DATE) IS NULL
             AND nb.resolved_note_body_date IS NULL THEN TRUE"""

    return f"""
CREATE OR REPLACE VIEW enriched_{entity_table} AS
WITH {anchor_ctes}
nb_dates AS (
    SELECT note_row_id, resolved_note_body_date
    FROM note_body_date_recovery_v
    WHERE date_is_plausible = TRUE
)
SELECT
    e.*,
    COALESCE(
        TRY_CAST(e.entity_date AS DATE),
        TRY_CAST(e.note_date AS DATE),
        nb.resolved_note_body_date,
        {surg_coalesce}
    ) AS inferred_event_date,
    CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
            THEN 'entity_date'
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL
            THEN 'note_date'
        WHEN nb.resolved_note_body_date IS NOT NULL THEN 'note_body_text'{surg_source_cases}
        ELSE 'unrecoverable'
    END AS date_source,
    CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'day'
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'day'
        WHEN nb.resolved_note_body_date IS NOT NULL THEN 'day'{surg_gran_cases}
        ELSE NULL
    END AS date_granularity,
    CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 100
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 70
        WHEN nb.resolved_note_body_date IS NOT NULL THEN 50{surg_conf_cases}
        ELSE 0
    END AS date_confidence,
    CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'extracted'
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'encounter'
        WHEN nb.resolved_note_body_date IS NOT NULL THEN 'note_body'{surg_anchor_cases}
        ELSE 'none'
    END AS date_anchor_type,
    CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN '{entity_table}'
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'clinical_notes_long'
        WHEN nb.resolved_note_body_date IS NOT NULL THEN 'clinical_notes_long'{surg_table_cases}
        ELSE 'none'
    END AS date_anchor_table,
    CASE
        WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL THEN 'exact_source_date'
        WHEN e.note_date IS NOT NULL AND TRY_CAST(e.note_date AS DATE) IS NOT NULL THEN 'inferred_day_level_date'
        WHEN nb.resolved_note_body_date IS NOT NULL THEN 'note_text_inferred_date'
        WHEN {surg_coalesce.split(',')[0].strip()} IS NOT NULL THEN 'coarse_anchor_date'
        ELSE 'unresolved_date'
    END AS date_status,
    CASE WHEN e.entity_date IS NOT NULL AND TRY_CAST(e.entity_date AS DATE) IS NOT NULL
        THEN TRUE ELSE FALSE END AS date_is_source_native_flag,
    CASE WHEN (e.entity_date IS NULL OR TRY_CAST(e.entity_date AS DATE) IS NULL)
              AND COALESCE(TRY_CAST(e.note_date AS DATE), nb.resolved_note_body_date,
                           {surg_coalesce}) IS NOT NULL
        THEN TRUE ELSE FALSE END AS date_is_inferred_flag,
    CASE
        WHEN e.entity_date IS NULL AND e.note_date IS NULL
             AND nb.resolved_note_body_date IS NULL
             AND {surg_coalesce.split(',')[0].strip()} IS NULL THEN TRUE{surg_review_check}
        ELSE FALSE
    END AS date_requires_manual_review_flag
FROM {entity_table} e
LEFT JOIN nb_dates nb ON e.note_row_id = nb.note_row_id{anchor_joins};
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  GAP 1 — Molecular Episode V3 (enhanced)
# ═══════════════════════════════════════════════════════════════════════════════

MOLECULAR_V3_SQL = """
CREATE OR REPLACE VIEW molecular_episode_v3 AS
WITH fna_per_patient AS (
    SELECT research_id, COUNT(*) AS fna_count
    FROM fna_history
    WHERE fna_date_parsed IS NOT NULL AND fna_date_parsed != ''
    GROUP BY research_id
),
date_classified AS (
    SELECT
        m.*,
        CASE
            WHEN m.specimen_date_raw IS NULL
                 OR TRIM(CAST(m.specimen_date_raw AS VARCHAR)) = ''
                THEN 'missing'
            WHEN TRY_CAST(m.specimen_date_raw AS DATE) IS NOT NULL
                THEN 'exact_valid_date'
            WHEN regexp_matches(CAST(m.specimen_date_raw AS VARCHAR), '^\\d{4}$')
                THEN 'year_only'
            WHEN LOWER(CAST(m.specimen_date_raw AS VARCHAR))
                 IN ('x','none','maybe?','n/a','na','unknown')
                THEN 'placeholder'
            WHEN TRY_CAST(
                    regexp_extract(CAST(m.specimen_date_raw AS VARCHAR),
                                   '\\b(\\d{1,2}/\\d{1,2}/\\d{2,4})\\b', 1)
                    AS DATE) IS NOT NULL
                THEN 'embedded_date_recoverable'
            ELSE 'garbage_unparseable'
        END AS raw_date_class
    FROM molecular_episode_v2 m
),
base AS (
    SELECT
        dc.*,
        CASE
            WHEN LOWER(TRIM(COALESCE(CAST(dc.platform_raw AS VARCHAR), ''))) IN ('x', '')
                 AND dc.result_category_normalized = 'missing'
                 AND dc.raw_date_class IN ('placeholder', 'missing')
                THEN TRUE
            ELSE FALSE
        END AS is_placeholder_row,
        CASE
            WHEN dc.raw_date_class IN ('garbage_unparseable', 'embedded_date_recoverable')
                THEN TRY_CAST(
                    regexp_extract(CAST(dc.specimen_date_raw AS VARCHAR),
                                   '\\b(\\d{1,2}/\\d{1,2}/\\d{2,4})\\b', 1) AS DATE)
            ELSE NULL
        END AS embedded_date_extracted,
        dc.raw_date_class AS molecular_date_raw_class_v2,
        CASE
            WHEN dc.molecular_date IS NOT NULL THEN TRUE
            WHEN dc.raw_date_class = 'embedded_date_recoverable' THEN TRUE
            ELSE FALSE
        END AS molecular_date_parse_success_flag,
        COALESCE(dc.molecular_date_granularity, 'none') AS molecular_date_precision,
        COALESCE(
            dc.molecular_date,
            CASE WHEN dc.raw_date_class IN ('garbage_unparseable','embedded_date_recoverable')
                THEN TRY_CAST(regexp_extract(CAST(dc.specimen_date_raw AS VARCHAR),
                              '\\b(\\d{1,2}/\\d{1,2}/\\d{2,4})\\b', 1) AS DATE)
                ELSE NULL END
        ) AS effective_molecular_date,
        COALESCE(fp.fna_count, 0) AS patient_fna_count,
        CASE
            WHEN dc.molecular_date IS NOT NULL AND dc.molecular_date_granularity = 'day'
                 AND dc.linked_fna_date IS NOT NULL
                 AND ABS(dc.days_molecular_to_fna) <= 30  THEN 95
            WHEN dc.molecular_date IS NOT NULL AND dc.molecular_date_granularity = 'day'
                 AND dc.linked_fna_date IS NOT NULL
                 AND ABS(dc.days_molecular_to_fna) <= 90  THEN 85
            WHEN dc.molecular_date IS NOT NULL AND dc.molecular_date_granularity = 'day'
                 AND dc.linked_fna_date IS NOT NULL        THEN 70
            WHEN dc.molecular_date IS NOT NULL AND dc.molecular_date_granularity = 'day'
                 AND dc.linked_surgery_date IS NOT NULL     THEN 60
            WHEN dc.molecular_date IS NOT NULL AND dc.molecular_date_granularity = 'year'
                 AND dc.linked_fna_date IS NOT NULL
                 AND EXTRACT(YEAR FROM dc.molecular_date) =
                     EXTRACT(YEAR FROM dc.linked_fna_date)  THEN 50
            WHEN dc.molecular_date IS NOT NULL AND dc.molecular_date_granularity = 'year'
                THEN 30
            WHEN dc.molecular_date IS NULL AND dc.linked_fna_date IS NOT NULL
                 AND COALESCE(fp.fna_count, 0) = 1          THEN 55
            WHEN dc.molecular_date IS NULL AND dc.linked_fna_date IS NOT NULL
                THEN 20
            ELSE 0
        END AS temporal_linkage_confidence,
        CASE
            WHEN dc.platform_normalized IN ('ThyroSeq', 'Afirma') THEN 90
            WHEN dc.platform_raw IS NOT NULL
                 AND TRIM(CAST(dc.platform_raw AS VARCHAR)) NOT IN ('', 'x', 'X')
                THEN 60
            ELSE 30
        END AS platform_confidence,
        CASE
            WHEN dc.linked_surgery_date IS NOT NULL AND dc.linked_histology IS NOT NULL
                THEN 80
            WHEN dc.linked_surgery_date IS NOT NULL THEN 60
            WHEN dc.linked_fna_date IS NOT NULL     THEN 50
            ELSE 10
        END AS pathology_concordance_confidence
    FROM date_classified dc
    LEFT JOIN fna_per_patient fp ON CAST(dc.research_id AS BIGINT) = fp.research_id
)
SELECT
    b.research_id, b.molecular_episode_id, b.specimen_date_raw,
    b.molecular_date, b.molecular_date_granularity, b.molecular_date_source,
    b.platform_raw, b.platform_normalized, b.test_name_raw,
    b.result_summary_raw, b.result_category_normalized,
    b.braf_flag, b.ras_flag, b.ret_flag, b.tert_flag,
    b.ntrk_flag, b.alk_flag, b.tp53_flag, b.pten_flag, b.fusion_flag,
    b.high_risk_molecular_flag,
    b.linked_fna_date, b.linked_fna_bethesda,
    b.linked_surgery_date, b.linked_pathology_date, b.linked_histology,
    b.days_molecular_to_fna, b.days_molecular_to_surgery,
    b.linkage_method, b.linkage_confidence, b.unresolved_flag,
    b.is_placeholder_row, b.embedded_date_extracted, b.effective_molecular_date,
    b.raw_date_class AS molecular_date_raw_class,
    b.molecular_date_raw_class_v2,
    b.molecular_date_parse_success_flag, b.molecular_date_precision,
    b.patient_fna_count,
    b.temporal_linkage_confidence, b.platform_confidence,
    b.pathology_concordance_confidence,
    ROUND((b.temporal_linkage_confidence * 0.50
         + b.platform_confidence * 0.20
         + b.pathology_concordance_confidence * 0.30), 0)::INTEGER
        AS overall_linkage_confidence,
    CASE
        WHEN b.is_placeholder_row THEN FALSE
        WHEN b.temporal_linkage_confidence >= 70
             AND b.platform_confidence >= 60
             AND b.pathology_concordance_confidence >= 50 THEN TRUE
        WHEN b.temporal_linkage_confidence >= 85
             AND b.platform_confidence >= 30             THEN TRUE
        WHEN b.temporal_linkage_confidence >= 55
             AND b.platform_confidence >= 60
             AND b.pathology_concordance_confidence >= 50 THEN TRUE
        ELSE FALSE
    END AS molecular_analysis_eligible_flag
FROM base b;
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  GAP 2 — RAI Episode V3 (note-text date extraction)
# ═══════════════════════════════════════════════════════════════════════════════

RAI_V3_SQL = """
CREATE OR REPLACE VIEW rai_episode_v3 AS
WITH surg_anchor AS (
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
-- Recover dates from clinical_notes_long note body for history_summary RAI mentions.
-- Deduplicate to one best date per (research_id, entity_value_raw) to avoid row multiplication.
note_text_dates_raw AS (
    SELECT
        e.research_id,
        e.note_row_id,
        e.entity_value_raw,
        TRY_CAST(
            regexp_extract(CAST(cn.note_text AS VARCHAR),
                           '\\b(\\d{1,2}/\\d{1,2}/\\d{2,4})\\b', 1)
            AS DATE
        ) AS text_date_full,
        regexp_extract(CAST(cn.note_text AS VARCHAR),
                       '\\b(\\d{1,2}/\\d{4})\\b', 1) AS text_date_month_year,
        regexp_extract(CAST(cn.note_text AS VARCHAR),
                       '(?:RAI|I-131|radioactive iodine|ablation)[^.]{0,40}(\\d{4})',
                       1) AS text_year_near_rai,
        cn.note_date AS cn_note_date,
        ROW_NUMBER() OVER (
            PARTITION BY e.research_id, e.entity_value_raw
            ORDER BY
                CASE WHEN TRY_CAST(regexp_extract(CAST(cn.note_text AS VARCHAR),
                     '\\b(\\d{1,2}/\\d{1,2}/\\d{2,4})\\b', 1) AS DATE) IS NOT NULL
                     THEN 0 ELSE 1 END,
                CASE WHEN regexp_extract(CAST(cn.note_text AS VARCHAR),
                     '\\b(\\d{1,2}/\\d{4})\\b', 1) != '' THEN 0 ELSE 1 END
        ) AS rn
    FROM note_entities_medications e
    JOIN clinical_notes_long cn ON e.note_row_id = cn.note_row_id
    WHERE (
        LOWER(e.entity_value_norm) LIKE '%rai%'
        OR LOWER(e.entity_value_norm) LIKE '%radioactive%'
        OR LOWER(e.entity_value_norm) LIKE '%i-131%'
        OR LOWER(e.entity_value_norm) LIKE '%i131%'
        OR LOWER(e.entity_value_norm) LIKE '%iodine%'
        OR LOWER(e.entity_value_norm) LIKE '%thyrogen%'
    )
    AND e.entity_date IS NULL
    AND e.note_date IS NULL
),
note_text_dates AS (
    SELECT * FROM note_text_dates_raw WHERE rn = 1
),
base AS (
    SELECT
        r.*,
        CASE
            WHEN r.rai_given_flag = 'negated' THEN 'negated'
            WHEN r.rai_given_flag = 'planned' THEN 'planned'
            WHEN r.rai_given_flag = 'historical' THEN 'historical'
            WHEN r.rai_given_flag = 'definite' AND r.dose_mci IS NOT NULL
                THEN 'definite_received'
            WHEN r.rai_given_flag = 'definite' AND r.post_thyroidectomy_flag = TRUE
                 AND r.days_surgery_to_rai BETWEEN 0 AND 365
                THEN 'definite_received'
            WHEN r.rai_given_flag = 'definite'
                THEN 'likely_received'
            ELSE 'ambiguous'
        END AS rai_assertion_status,

        CASE
            WHEN r.rai_given_flag = 'negated' THEN 0
            WHEN r.rai_given_flag = 'definite' AND r.dose_mci IS NOT NULL
                 AND r.post_thyroidectomy_flag = TRUE
                 AND r.days_surgery_to_rai BETWEEN 0 AND 365 THEN 95
            WHEN r.rai_given_flag = 'definite' AND r.post_thyroidectomy_flag = TRUE
                 AND r.days_surgery_to_rai BETWEEN 0 AND 365 THEN 85
            WHEN r.rai_given_flag = 'definite' AND r.post_thyroidectomy_flag = TRUE
                THEN 65
            WHEN r.rai_given_flag = 'definite' THEN 50
            WHEN r.rai_given_flag = 'planned' THEN 30
            WHEN r.rai_given_flag = 'historical' THEN 40
            ELSE 20
        END AS rai_treatment_certainty,

        CASE
            WHEN r.rai_date IS NULL THEN 'missing_rai_date'
            WHEN r.linked_surgery_date IS NULL THEN 'missing_surgery_anchor'
            WHEN r.days_surgery_to_rai < -30 THEN 'pre_index_surgery'
            WHEN r.days_surgery_to_rai > 730 THEN 'likely_recurrence_related'
            WHEN r.days_surgery_to_rai > 365 THEN 'too_remote_from_index_surgery'
            ELSE 'plausible_index_treatment'
        END AS rai_interval_class,

        sa.surg_date AS patient_primary_surgery_date,

        -- Note-text date recovery
        COALESCE(
            ntd.text_date_full,
            TRY_CAST(ntd.text_date_month_year || '/01' AS DATE),
            TRY_CAST(ntd.text_year_near_rai || '-07-01' AS DATE)
        ) AS note_text_recovered_date,
        CASE
            WHEN ntd.text_date_full IS NOT NULL THEN 'note_text_full_date'
            WHEN ntd.text_date_month_year IS NOT NULL THEN 'note_text_month_year'
            WHEN ntd.text_year_near_rai IS NOT NULL THEN 'note_text_year_near_rai'
            ELSE NULL
        END AS note_text_date_source,
        CASE
            WHEN ntd.text_date_full IS NOT NULL THEN 'day'
            WHEN ntd.text_date_month_year IS NOT NULL THEN 'month'
            WHEN ntd.text_year_near_rai IS NOT NULL THEN 'year'
            ELSE NULL
        END AS note_text_date_precision,

        -- Effective date: use original if available, else note-text recovery
        COALESCE(
            r.rai_date,
            ntd.text_date_full,
            TRY_CAST(ntd.text_date_month_year || '/01' AS DATE),
            TRY_CAST(ntd.text_year_near_rai || '-07-01' AS DATE)
        ) AS effective_rai_date,

        CASE
            WHEN r.rai_date IS NOT NULL THEN 'original'
            WHEN ntd.text_date_full IS NOT NULL THEN 'note_text_recovered'
            WHEN ntd.text_date_month_year IS NOT NULL THEN 'note_text_recovered'
            WHEN ntd.text_year_near_rai IS NOT NULL THEN 'note_text_recovered'
            WHEN sa.surg_date IS NOT NULL THEN 'patient_anchor_only'
            ELSE 'no_anchor_available'
        END AS rai_date_recovery_status

    FROM rai_episode_v2 r
    LEFT JOIN surg_anchor sa
        ON CAST(r.research_id AS BIGINT) = sa.research_id AND sa.op_seq = 1
    LEFT JOIN note_text_dates ntd
        ON CAST(r.research_id AS BIGINT) = CAST(ntd.research_id AS BIGINT)
        AND r.source_table = 'note_entities_medications'
        AND CAST(r.dose_raw AS VARCHAR) = CAST(ntd.entity_value_raw AS VARCHAR)
)
SELECT
    b.*,
    CASE
        WHEN b.rai_assertion_status IN ('definite_received', 'likely_received')
             AND b.rai_interval_class = 'plausible_index_treatment'
            THEN TRUE
        WHEN b.rai_assertion_status = 'definite_received'
             AND b.rai_interval_class = 'too_remote_from_index_surgery'
            THEN TRUE
        -- Note-text recovered date: if within plausible window of surgery
        WHEN b.rai_assertion_status IN ('definite_received', 'likely_received')
             AND b.note_text_recovered_date IS NOT NULL
             AND b.patient_primary_surgery_date IS NOT NULL
             AND DATE_DIFF('day', b.patient_primary_surgery_date, b.note_text_recovered_date)
                 BETWEEN -30 AND 730
            THEN TRUE
        -- Patient-anchor: undated mention + surgery + definite/likely
        WHEN b.rai_assertion_status IN ('definite_received', 'likely_received')
             AND b.rai_date_recovery_status = 'patient_anchor_only'
             AND b.rai_treatment_certainty >= 50
            THEN TRUE
        ELSE FALSE
    END AS rai_eligible_for_analysis_flag
FROM base b;
"""


# ═══════════════════════════════════════════════════════════════════════════════
#  GAP 3 — Histology Analysis Cohort (calculated staging + tiered eligibility)
# ═══════════════════════════════════════════════════════════════════════════════

HISTOLOGY_COHORT_SQL = """
CREATE OR REPLACE VIEW histology_analysis_cohort_v AS
WITH staged AS (
    SELECT
        h.*,

        -- Parse tumor size to numeric
        TRY_CAST(
            regexp_extract(CAST(h.largest_focus_cm AS VARCHAR), '([\\d.]+)', 1)
            AS DOUBLE
        ) AS tumor_size_cm_numeric,

        -- Normalize ETE
        CASE
            WHEN LOWER(CAST(h.tumor_1_extrathyroidal_extension AS VARCHAR))
                 IN ('present','yes','extensive','gross','yes;') THEN 'gross'
            WHEN LOWER(CAST(h.tumor_1_extrathyroidal_extension AS VARCHAR))
                 IN ('minimal','microscopic','focal','yes, minimal',
                     'yes (minimal)') THEN 'microscopic'
            WHEN LOWER(CAST(h.tumor_1_extrathyroidal_extension AS VARCHAR))
                 IN ('x','none','no','absent','indeterminate','')
                 OR h.tumor_1_extrathyroidal_extension IS NULL THEN 'absent'
            WHEN LOWER(CAST(h.tumor_1_extrathyroidal_extension AS VARCHAR))
                 LIKE '%c/a%' THEN 'absent'
            ELSE 'absent'
        END AS ete_normalized,

        -- Parse LN involved to numeric
        TRY_CAST(
            regexp_extract(CAST(h.tumor_1_ln_involved AS VARCHAR), '(\\d+)', 1)
            AS INTEGER
        ) AS ln_positive_numeric,

        -- Parse LN examined
        TRY_CAST(
            regexp_extract(CAST(h.tumor_1_ln_examined AS VARCHAR), '(\\d+)', 1)
            AS INTEGER
        ) AS ln_examined_numeric

    FROM histology_reconciliation_v2 h
),
calculated AS (
    SELECT
        s.*,

        -- AJCC 8th Ed T-stage calculation (DTC: PTC, FTC, HCC, PDTC)
        -- Microscopic ETE does NOT upstage in AJCC8; only gross ETE does.
        CASE
            WHEN s.histology_normalized IN ('MTC','ATC') THEN NULL
            WHEN s.tumor_size_cm_numeric IS NULL THEN NULL
            WHEN s.ete_normalized = 'gross' THEN 'T3b'
            WHEN s.tumor_size_cm_numeric <= 1.0 THEN 'T1a'
            WHEN s.tumor_size_cm_numeric <= 2.0 THEN 'T1b'
            WHEN s.tumor_size_cm_numeric <= 4.0 THEN 'T2'
            WHEN s.tumor_size_cm_numeric > 4.0 THEN 'T3a'
            ELSE NULL
        END AS calculated_t_stage,

        -- N-stage: N0 vs N1 (can't distinguish N1a/N1b without compartment)
        CASE
            WHEN s.ln_positive_numeric IS NOT NULL AND s.ln_positive_numeric = 0 THEN 'N0'
            WHEN s.ln_positive_numeric IS NOT NULL AND s.ln_positive_numeric > 0 THEN 'N1'
            WHEN s.ln_examined_numeric IS NOT NULL AND s.ln_examined_numeric > 0
                 AND s.ln_positive_numeric IS NULL THEN 'Nx_examined'
            ELSE NULL
        END AS calculated_n_stage,

        CASE
            WHEN s.tumor_size_cm_numeric IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS t_stage_calculable_flag,
        CASE
            WHEN s.ln_positive_numeric IS NOT NULL THEN TRUE
            WHEN s.ln_examined_numeric IS NOT NULL AND s.ln_examined_numeric > 0 THEN TRUE
            ELSE FALSE
        END AS n_stage_calculable_flag

    FROM staged s
)
SELECT
    c.research_id,
    c.op_seq,
    c.pathology_date,

    COALESCE(c.histology_normalized, 'unclassified') AS final_histology_for_analysis,
    c.variant_normalized AS final_variant_for_analysis,
    -- Use formal staging if available, else fall back to calculated
    COALESCE(c.t_stage_reconciled, c.t_stage_source_note,
             c.calculated_t_stage) AS final_t_stage_for_analysis,
    COALESCE(c.n_stage_reconciled, c.n_stage_source_note,
             c.calculated_n_stage) AS final_n_stage_for_analysis,
    COALESCE(c.laterality_reconciled, 'unknown') AS final_laterality_for_analysis,
    c.tumor_behavior_category,

    -- Staging source tracking
    CASE
        WHEN c.t_stage_reconciled IS NOT NULL THEN 'formal_pathology'
        WHEN c.t_stage_source_note IS NOT NULL THEN 'clinical_note'
        WHEN c.calculated_t_stage IS NOT NULL THEN 'calculated_ajcc8'
        ELSE 'unavailable'
    END AS t_stage_source,
    CASE
        WHEN c.n_stage_reconciled IS NOT NULL THEN 'formal_pathology'
        WHEN c.n_stage_source_note IS NOT NULL THEN 'clinical_note'
        WHEN c.calculated_n_stage IS NOT NULL THEN 'calculated_ajcc8'
        ELSE 'unavailable'
    END AS n_stage_source,

    -- Calculated staging detail
    c.calculated_t_stage,
    c.calculated_n_stage,
    c.tumor_size_cm_numeric,
    c.ete_normalized,
    c.ln_positive_numeric,
    c.ln_examined_numeric,
    c.t_stage_calculable_flag,
    c.n_stage_calculable_flag,

    -- Raw values preserved
    c.source_histology_raw_ps, c.source_histology_raw_tp, c.source_histology_raw,
    c.t_stage_source_path, c.t_stage_tp, c.t_stage_source_note,
    c.n_stage_source_path, c.n_stage_tp, c.n_stage_source_note,
    c.overall_stage_tp, c.note_overall_stage,
    c.laterality_path, c.multifocality_path, c.largest_focus_cm,
    c.tumor_1_extrathyroidal_extension,
    c.source_priority_used,

    -- Discordance typing
    CASE
        WHEN c.histology_discordance_flag THEN 'histology_type_conflict'
        WHEN c.stage_discordance_flag AND c.n_stage_discordance_flag
            THEN 't_stage_conflict,n_stage_conflict'
        WHEN c.stage_discordance_flag THEN 't_stage_conflict'
        WHEN c.n_stage_discordance_flag THEN 'n_stage_conflict'
        WHEN c.laterality_reconciled IS NULL AND c.pathology_date IS NOT NULL
            THEN 'laterality_conflict'
        WHEN c.histology_normalized IS NULL AND c.source_histology_raw IS NOT NULL
            THEN 'note_only_histology'
        WHEN c.t_stage_reconciled IS NULL AND c.t_stage_source_note IS NOT NULL
            THEN 'missing_path_primary'
        ELSE NULL
    END AS discordance_type,

    CASE
        WHEN c.histology_discordance_flag OR c.stage_discordance_flag THEN TRUE
        ELSE FALSE
    END AS adjudication_needed_flag,

    -- Full eligibility (unchanged logic)
    CASE
        WHEN c.unresolved_flag AND c.calculated_t_stage IS NULL THEN FALSE
        WHEN c.histology_normalized IS NULL THEN FALSE
        WHEN c.histology_discordance_flag THEN FALSE
        ELSE TRUE
    END AS analysis_eligible_flag,

    -- Histology-only tier
    CASE
        WHEN c.histology_normalized IS NULL THEN FALSE
        WHEN c.histology_discordance_flag THEN FALSE
        ELSE TRUE
    END AS analysis_eligible_histology_only_flag,

    -- Eligibility tier
    CASE
        WHEN c.histology_normalized IS NULL THEN 'ineligible_no_histology'
        WHEN c.histology_discordance_flag THEN 'ineligible_discordant'
        WHEN NOT c.unresolved_flag AND NOT c.histology_discordance_flag
            THEN 'eligible_full'
        WHEN c.calculated_t_stage IS NOT NULL AND NOT c.histology_discordance_flag
            THEN 'eligible_calculated_staging'
        WHEN c.histology_normalized IS NOT NULL AND NOT c.histology_discordance_flag
            THEN 'eligible_histology_only'
        ELSE 'ineligible_other'
    END AS eligibility_tier,

    c.histology_discordance_flag,
    c.stage_discordance_flag,
    c.n_stage_discordance_flag,
    c.unresolved_flag,
    c.reconciliation_status

FROM calculated c;
"""


# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description="Gap Remediation")
    parser.add_argument("--md", action="store_true")
    args = parser.parse_args()

    print("=" * 80)
    print("  GAP REMEDIATION — Molecular / RAI / Histology")
    print("  Mode: " + ("MotherDuck" if args.md else "Local DuckDB"))
    print("=" * 80)

    if args.md:
        from motherduck_client import MotherDuckClient
        client = MotherDuckClient()
        con = client.connect_rw()
    else:
        con = duckdb.connect(str(DB_PATH))

    section("REGISTERING DATA SOURCES")
    if not args.md:
        register_parquets(con)

    # Baseline
    section("BASELINE (BEFORE)")
    baselines = {
        "enriched_unresolved": """SELECT COUNT(*) FROM (
            SELECT 1 FROM enriched_note_entities_genetics WHERE date_status = 'unresolved_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_staging WHERE date_status = 'unresolved_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_procedures WHERE date_status = 'unresolved_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_complications WHERE date_status = 'unresolved_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_medications WHERE date_status = 'unresolved_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_problem_list WHERE date_status = 'unresolved_date'
        ) x""",
        "enriched_coarse": """SELECT COUNT(*) FROM (
            SELECT 1 FROM enriched_note_entities_genetics WHERE date_status = 'coarse_anchor_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_staging WHERE date_status = 'coarse_anchor_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_procedures WHERE date_status = 'coarse_anchor_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_complications WHERE date_status = 'coarse_anchor_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_medications WHERE date_status = 'coarse_anchor_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_problem_list WHERE date_status = 'coarse_anchor_date'
        ) x""",
        "mol_eligible": "SELECT COUNT(*) FROM molecular_analysis_cohort_v",
        "rai_eligible": "SELECT COUNT(*) FROM rai_analysis_cohort_v",
        "hist_eligible_full": """SELECT COUNT(*) FROM histology_analysis_cohort_v
                                 WHERE analysis_eligible_flag = TRUE""",
        "hist_with_staging": """SELECT COUNT(*) FROM histology_analysis_cohort_v
                                WHERE final_t_stage_for_analysis IS NOT NULL""",
    }
    before = {}
    for label, sql in baselines.items():
        try:
            cnt = con.execute(sql).fetchone()[0]
            before[label] = cnt
            print(f"  {label:<45} {cnt:>8,}")
        except Exception as e:
            before[label] = -1
            print(f"  {label:<45} ERROR: {e}")

    # Deploy: note-body date recovery helper
    section("GAP 0: NOTE-BODY DATE RECOVERY (all domains)")
    deploy_view(con, "note_body_date_recovery_v", NOTE_BODY_DATE_RECOVERY_SQL)

    # Deploy: updated enriched views with note-body fallback
    section("UPDATING ENRICHED VIEWS (note-body date fallback)")
    # staging/procedures/complications: surgery-only fallback
    for tbl in ["note_entities_staging", "note_entities_procedures",
                "note_entities_complications"]:
        deploy_view(con, f"enriched_{tbl}",
                    _enriched_with_note_body_view(tbl, has_surg_fna=False))
    # genetics: surgery + FNA + molecular fallback (keep existing chain)
    deploy_view(con, "enriched_note_entities_genetics",
                _enriched_with_note_body_view("note_entities_genetics", has_surg_fna=True))
    # medications/problem_list: surgery + FNA fallback
    for tbl in ["note_entities_medications", "note_entities_problem_list"]:
        deploy_view(con, f"enriched_{tbl}",
                    _enriched_with_note_body_view(tbl, has_surg_fna=True))

    # Deploy
    section("GAP 1: MOLECULAR — placeholder + embedded dates + single-FNA")
    deploy_view(con, "molecular_episode_v3", MOLECULAR_V3_SQL)
    deploy_view(con, "molecular_analysis_cohort_v",
                "CREATE OR REPLACE VIEW molecular_analysis_cohort_v AS "
                "SELECT * FROM molecular_episode_v3 WHERE molecular_analysis_eligible_flag = TRUE;")

    section("GAP 2: RAI — note-text date extraction")
    deploy_view(con, "rai_episode_v3", RAI_V3_SQL)
    deploy_view(con, "rai_analysis_cohort_v",
                "CREATE OR REPLACE VIEW rai_analysis_cohort_v AS "
                "SELECT * FROM rai_episode_v3 WHERE rai_eligible_for_analysis_flag = TRUE;")

    section("GAP 3: HISTOLOGY — calculated AJCC8 staging + tiered eligibility")
    deploy_view(con, "histology_analysis_cohort_v", HISTOLOGY_COHORT_SQL)

    # After
    section("AFTER REMEDIATION")
    after_queries = {
        "enriched_unresolved": """SELECT COUNT(*) FROM (
            SELECT 1 FROM enriched_note_entities_genetics WHERE date_status = 'unresolved_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_staging WHERE date_status = 'unresolved_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_procedures WHERE date_status = 'unresolved_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_complications WHERE date_status = 'unresolved_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_medications WHERE date_status = 'unresolved_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_problem_list WHERE date_status = 'unresolved_date'
        ) x""",
        "enriched_note_text_inferred": """SELECT COUNT(*) FROM (
            SELECT 1 FROM enriched_note_entities_genetics WHERE date_status = 'note_text_inferred_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_staging WHERE date_status = 'note_text_inferred_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_procedures WHERE date_status = 'note_text_inferred_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_complications WHERE date_status = 'note_text_inferred_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_medications WHERE date_status = 'note_text_inferred_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_problem_list WHERE date_status = 'note_text_inferred_date'
        ) x""",
        "enriched_coarse": """SELECT COUNT(*) FROM (
            SELECT 1 FROM enriched_note_entities_genetics WHERE date_status = 'coarse_anchor_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_staging WHERE date_status = 'coarse_anchor_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_procedures WHERE date_status = 'coarse_anchor_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_complications WHERE date_status = 'coarse_anchor_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_medications WHERE date_status = 'coarse_anchor_date'
            UNION ALL SELECT 1 FROM enriched_note_entities_problem_list WHERE date_status = 'coarse_anchor_date'
        ) x""",
        "mol_eligible": "SELECT COUNT(*) FROM molecular_analysis_cohort_v",
        "mol_placeholder_detected": """SELECT COUNT(*) FROM molecular_episode_v3
                                       WHERE is_placeholder_row = TRUE""",
        "mol_real_non_placeholder": """SELECT COUNT(*) FROM molecular_episode_v3
                                       WHERE is_placeholder_row = FALSE""",
        "rai_eligible": "SELECT COUNT(*) FROM rai_analysis_cohort_v",
        "rai_no_anchor_remaining": """SELECT COUNT(*) FROM rai_episode_v3
                                      WHERE rai_date_recovery_status = 'no_anchor_available'""",
        "hist_eligible_full": """SELECT COUNT(*) FROM histology_analysis_cohort_v
                                 WHERE analysis_eligible_flag = TRUE""",
        "hist_eligible_hist_only": """SELECT COUNT(*) FROM histology_analysis_cohort_v
                                      WHERE analysis_eligible_histology_only_flag = TRUE""",
        "hist_with_any_staging": """SELECT COUNT(*) FROM histology_analysis_cohort_v
                                    WHERE final_t_stage_for_analysis IS NOT NULL""",
    }
    after = {}
    for label, sql in after_queries.items():
        try:
            cnt = con.execute(sql).fetchone()[0]
            after[label] = cnt
            print(f"  {label:<45} {cnt:>8,}")
        except Exception as e:
            after[label] = -1
            print(f"  {label:<45} ERROR: {e}")

    # Eligibility tiers
    print()
    try:
        rows = con.execute("""
            SELECT eligibility_tier, COUNT(*) FROM histology_analysis_cohort_v
            GROUP BY 1 ORDER BY 1
        """).fetchall()
        print("  Histology eligibility tiers:")
        for r in rows:
            print(f"    {r[0]:<35} {r[1]:>8,}")
    except Exception as e:
        print(f"  ERROR: {e}")

    # RAI date recovery detail
    print()
    try:
        rows = con.execute("""
            SELECT rai_date_recovery_status, note_text_date_precision,
                   COUNT(*) as n, COUNT(DISTINCT CAST(research_id AS BIGINT)) as pts
            FROM rai_episode_v3
            GROUP BY 1, 2 ORDER BY n DESC
        """).fetchall()
        print("  RAI date recovery status:")
        for r in rows:
            print(f"    {r[0]:<25} precision={str(r[1]):<10} {r[2]:>5} rows  {r[3]:>5} patients")
    except Exception as e:
        print(f"  ERROR: {e}")

    # Comparison
    section("BEFORE / AFTER COMPARISON")
    print(f"  {'metric':<45} {'before':>8} {'after':>8} {'delta':>8}")
    print("  " + "-" * 71)
    for label, b_key, a_key in [
        ("Enriched: unresolved_date", "enriched_unresolved", "enriched_unresolved"),
        ("Enriched: coarse_anchor_date", "enriched_coarse", "enriched_coarse"),
        ("Enriched: note_text_inferred (NEW)", "enriched_unresolved", "enriched_note_text_inferred"),
        ("Molecular: analysis eligible", "mol_eligible", "mol_eligible"),
        ("RAI: analysis eligible", "rai_eligible", "rai_eligible"),
        ("Histology: eligible (full)", "hist_eligible_full", "hist_eligible_full"),
        ("Histology: with any T-staging", "hist_with_staging", "hist_with_any_staging"),
    ]:
        b = before.get(b_key, -1)
        a = after.get(a_key, -1)
        delta = a - b if b >= 0 and a >= 0 else "N/A"
        print(f"  {label:<45} {b:>8,} {a:>8,} {str(delta):>8}")

    # Remaining true gaps
    section("REMAINING TRUE SOURCE-DATA GAPS")
    gaps = [
        ("Molecular: placeholder stubs (not real tests)",
         "SELECT COUNT(*) FROM molecular_episode_v3 WHERE is_placeholder_row = TRUE"),
        ("Molecular: ineligible non-placeholder",
         "SELECT COUNT(*) FROM molecular_episode_v3 WHERE NOT is_placeholder_row AND NOT molecular_analysis_eligible_flag"),
        ("RAI: no anchor at all",
         "SELECT COUNT(*) FROM rai_episode_v3 WHERE rai_date_recovery_status = 'no_anchor_available'"),
        ("Histology: no histology type (benign/completion procedures)",
         "SELECT COUNT(*) FROM histology_analysis_cohort_v WHERE eligibility_tier = 'ineligible_no_histology'"),
    ]
    for label, sql in gaps:
        try:
            cnt = con.execute(sql).fetchone()[0]
            print(f"  {label:<65} {cnt:>6,}")
        except Exception as e:
            print(f"  {label:<65} ERROR")

    con.close()
    print(f"\n{'=' * 80}")
    print("  DONE — Gap Remediation complete")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
