#!/usr/bin/env python3
"""
80_structural_gap_maximization.py -- Final structural-gap maximization pass

Phases:
  A: RAI hardening -- coverage validation, source-limitation classification,
     nuclear-medicine absence as first-class category
  B: Recurrence hardening -- readiness table separating flagged/date-bearing/usable,
     date-quality taxonomy verification
  C: Non-Tg lab temporal truth -- analysis suitability per lab type,
     explicit encoding of date-source limitations
  D: Operative semantics -- companion parse-status fields for boolean columns,
     distinguishing FALSE from NOT_PARSED / SOURCE_ABSENT / UNKNOWN
  E: Documentation + exports

Supports --md (MotherDuck), --local, --dry-run, --phase A/B/C/D/E/all.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
sys.path.insert(0, str(ROOT))

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
DATE_STAMP = datetime.now().strftime("%Y%m%d")
EXPORT_DIR = ROOT / "exports" / f"structural_gap_maximization_{TIMESTAMP}"


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
# Phase A: RAI Structural Coverage
# ─────────────────────────────────────────────────────────────────────────────

RAI_COVERAGE_SQL = """
CREATE OR REPLACE TABLE val_rai_structural_coverage_v1 AS
WITH episode_stats AS (
    SELECT
        COUNT(*)                                                              AS total_episodes,
        COUNT(DISTINCT research_id)                                           AS total_patients,
        COUNT(*) FILTER (WHERE rai_assertion_status IN
            ('definite_received','likely_received'))                           AS confirmed_episodes,
        COUNT(DISTINCT research_id) FILTER (WHERE rai_assertion_status IN
            ('definite_received','likely_received'))                           AS confirmed_patients,
        COUNT(*) FILTER (WHERE dose_mci IS NOT NULL)                          AS episodes_with_dose,
        COUNT(DISTINCT research_id) FILTER (WHERE dose_mci IS NOT NULL)       AS patients_with_dose,
        COUNT(*) FILTER (WHERE resolved_rai_date IS NOT NULL)                 AS episodes_with_date,
        COUNT(*) FILTER (WHERE date_status = 'exact_source_date')             AS exact_date_episodes,
        COUNT(*) FILTER (WHERE date_status = 'inferred_day_level_date')       AS inferred_date_episodes,
        COUNT(*) FILTER (WHERE date_status = 'unresolved_date'
                            OR resolved_rai_date IS NULL)                      AS unresolved_date_episodes,
        COUNT(*) FILTER (WHERE dose_missingness_reason = 'dose_available')    AS dose_available_ct,
        COUNT(*) FILTER (WHERE dose_missingness_reason =
            'source_present_no_dose_stated')                                  AS dose_source_no_value,
        COUNT(*) FILTER (WHERE dose_missingness_reason = 'linkage_failed')    AS dose_linkage_failed,
        COUNT(*) FILTER (WHERE dose_missingness_reason =
            'no_source_report_available')                                     AS dose_no_source,
        AVG(dose_mci) FILTER (WHERE dose_mci IS NOT NULL)                     AS avg_dose_mci,
        MEDIAN(dose_mci) FILTER (WHERE dose_mci IS NOT NULL)                  AS median_dose_mci
    FROM rai_treatment_episode_v2
),
validated_stats AS (
    SELECT
        COUNT(*)                                                              AS validated_patients,
        COUNT(*) FILTER (WHERE rai_validation_tier = 'confirmed_with_dose')   AS tier_confirmed_dose,
        COUNT(*) FILTER (WHERE rai_validation_tier = 'confirmed_no_dose')     AS tier_confirmed_nodose,
        COUNT(*) FILTER (WHERE rai_validation_tier = 'unconfirmed_with_dose') AS tier_unconfirmed_dose,
        COUNT(*) FILTER (WHERE rai_validation_tier = 'unconfirmed_no_dose')   AS tier_unconfirmed_nodose,
        COUNT(*) FILTER (WHERE rai_validation_tier = 'no_rai')                AS tier_no_rai
    FROM extracted_rai_validated_v1
),
note_coverage AS (
    SELECT
        COUNT(DISTINCT research_id) FILTER (WHERE note_type = 'endocrine_note')  AS pts_endocrine,
        COUNT(DISTINCT research_id) FILTER (WHERE note_type = 'nuclear_med')     AS pts_nuclear_med,
        COUNT(DISTINCT research_id) FILTER (WHERE note_type = 'dc_sum')          AS pts_dc_sum,
        COUNT(DISTINCT research_id) FILTER (WHERE note_type = 'op_note')         AS pts_op_note
    FROM clinical_notes_long
),
surgical_cohort AS (
    SELECT COUNT(DISTINCT research_id) AS total_surgical_patients
    FROM path_synoptics
)
SELECT
    UNNEST([
        'total_rai_episodes',
        'total_rai_patients',
        'confirmed_episodes',
        'confirmed_patients',
        'episodes_with_dose',
        'patients_with_dose',
        'episodes_with_date',
        'exact_date_episodes',
        'inferred_date_episodes',
        'unresolved_date_episodes',
        'dose_available',
        'dose_source_no_value',
        'dose_linkage_failed',
        'dose_no_source_report',
        'avg_dose_mci',
        'median_dose_mci',
        'validated_patients_total',
        'tier_confirmed_with_dose',
        'tier_confirmed_no_dose',
        'tier_unconfirmed_with_dose',
        'tier_unconfirmed_no_dose',
        'tier_no_rai',
        'pts_with_endocrine_notes',
        'pts_with_nuclear_med_notes',
        'pts_with_dc_sum_notes',
        'pts_with_op_notes',
        'total_surgical_cohort'
    ]) AS metric_name,
    UNNEST([
        e.total_episodes,
        e.total_patients,
        e.confirmed_episodes,
        e.confirmed_patients,
        e.episodes_with_dose,
        e.patients_with_dose,
        e.episodes_with_date,
        e.exact_date_episodes,
        e.inferred_date_episodes,
        e.unresolved_date_episodes,
        e.dose_available_ct,
        e.dose_source_no_value,
        e.dose_linkage_failed,
        e.dose_no_source,
        CAST(ROUND(e.avg_dose_mci, 1) AS DOUBLE),
        CAST(ROUND(e.median_dose_mci, 1) AS DOUBLE),
        v.validated_patients,
        v.tier_confirmed_dose,
        v.tier_confirmed_nodose,
        v.tier_unconfirmed_dose,
        v.tier_unconfirmed_nodose,
        v.tier_no_rai,
        n.pts_endocrine,
        n.pts_nuclear_med,
        n.pts_dc_sum,
        n.pts_op_note,
        sc.total_surgical_patients
    ]) AS metric_value,
    UNNEST([
        'Total RAI mention episodes in rai_treatment_episode_v2',
        'Distinct patients with any RAI episode',
        'Episodes with definite_received or likely_received assertion',
        'Patients with confirmed RAI receipt',
        'Episodes where dose_mci is non-NULL',
        'Patients with any recorded dose',
        'Episodes with a resolved date',
        'Episodes with exact_source_date quality',
        'Episodes with inferred_day_level_date quality',
        'Episodes with unresolved or NULL date',
        'Episodes where dose is available (dose_missingness_reason)',
        'Source note present but no dose value stated',
        'Dose linkage to episode failed',
        'No source report available for dose extraction',
        'Average dose among episodes with dose (mCi)',
        'Median dose among episodes with dose (mCi)',
        'Patients in extracted_rai_validated_v1',
        'Validated tier: confirmed_with_dose',
        'Validated tier: confirmed_no_dose',
        'Validated tier: unconfirmed_with_dose',
        'Validated tier: unconfirmed_no_dose',
        'Validated tier: no_rai',
        'Patients with endocrine notes (primary RAI dose source)',
        'Patients with nuclear medicine notes (0 = SOURCE_ABSENT)',
        'Patients with discharge summary notes',
        'Patients with operative notes',
        'Total surgical patients (denominator)'
    ]) AS metric_description,
    UNNEST([
        CASE WHEN e.total_episodes > 0 THEN 'event_level' ELSE 'n/a' END,
        'patient_level', 'event_level', 'patient_level',
        'event_level', 'patient_level',
        'event_level', 'event_level', 'event_level', 'event_level',
        'dose_missingness', 'dose_missingness', 'dose_missingness', 'dose_missingness',
        'dose_summary', 'dose_summary',
        'validation_tier', 'validation_tier', 'validation_tier',
        'validation_tier', 'validation_tier', 'validation_tier',
        'note_coverage', 'note_coverage', 'note_coverage', 'note_coverage',
        'denominator'
    ]) AS metric_category,
    UNNEST([
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, 'source_present_dose_not_stated', 'linkage_failed', 'nuclear_medicine_reports_absent',
        NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, 'nuclear_medicine_reports_absent', NULL, NULL, NULL
    ]) AS source_limitation_category,
    CURRENT_TIMESTAMP AS audited_at
FROM episode_stats e, validated_stats v, note_coverage n, surgical_cohort sc
"""

RAI_SOURCE_LIMITATION_SQL = """
CREATE OR REPLACE TABLE val_rai_source_limitation_v1 AS
SELECT
    UNNEST([
        'nuclear_medicine_reports',
        'endocrine_clinic_notes',
        'discharge_summaries',
        'operative_notes',
        'structured_rai_orders'
    ]) AS source_domain,
    UNNEST([
        'ABSENT',
        'PARTIAL',
        'PARTIAL',
        'PARTIAL',
        'ABSENT'
    ]) AS availability_status,
    UNNEST([
        'Zero nuclear medicine notes exist in clinical_notes_long corpus. '
        'RAI dose/scan data relies entirely on endocrine and discharge note NLP. '
        'This is an institutional data feed limitation, not a pipeline gap.',
        'Endocrine notes present for subset of patients. Primary source for RAI '
        'assertion status and dose. Coverage depends on clinic follow-up patterns.',
        'Discharge summaries cover a small fraction of patients (1.6% note coverage). '
        'Occasionally contain RAI planning information but rarely dose details.',
        'Operative notes do not typically document RAI treatment (post-surgical). '
        'May contain pre-operative RAI history references.',
        'No structured RAI order/administration table exists in the source data. '
        'All RAI data is derived from NLP extraction of clinical notes.'
    ]) AS limitation_description,
    UNNEST([
        'RAI dose coverage ceiling is ~41% without nuclear medicine reports. '
        'Cannot improve further without institutional nuclear medicine data feed.',
        'Coverage expandable with additional endocrine clinic note inclusion. '
        'Current extraction captures definite/likely/planned/historical assertions.',
        'Minimal additional RAI data expected from this source.',
        'Not a primary RAI data source. Low yield expected.',
        'Would require institutional pharmacy/nuclear medicine order system integration.'
    ]) AS remediation_path,
    UNNEST([
        'first_class_structural_limitation',
        'partial_coverage',
        'minimal_yield',
        'not_primary_source',
        'first_class_structural_limitation'
    ]) AS limitation_tier,
    CURRENT_TIMESTAMP AS audited_at
"""


def phase_a(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase A: RAI Structural Coverage Hardening")
    results: dict = {}

    if not table_exists(con, "rai_treatment_episode_v2"):
        print("  [SKIP] rai_treatment_episode_v2 not found")
        return results

    before_ct = safe_count(con, "SELECT COUNT(*) FROM rai_treatment_episode_v2")
    print(f"  RAI episodes: {before_ct}")

    print("  Creating val_rai_structural_coverage_v1...")
    safe_exec(con, RAI_COVERAGE_SQL, dry)
    ct = safe_count(con, "SELECT COUNT(*) FROM val_rai_structural_coverage_v1")
    print(f"  -> {ct} coverage metrics")
    results["rai_coverage_metrics"] = ct

    print("  Creating val_rai_source_limitation_v1...")
    safe_exec(con, RAI_SOURCE_LIMITATION_SQL, dry)
    ct = safe_count(con, "SELECT COUNT(*) FROM val_rai_source_limitation_v1")
    print(f"  -> {ct} source limitation entries")
    results["rai_source_limitations"] = ct

    if not dry:
        df = con.execute("SELECT metric_name, metric_value, metric_category "
                         "FROM val_rai_structural_coverage_v1 "
                         "ORDER BY metric_category, metric_name").fetchdf()
        for _, row in df.iterrows():
            v = row["metric_value"]
            v_str = f"{int(v):,}" if v == int(v) else f"{v:.1f}"
            print(f"    {row['metric_category']:20s} | {row['metric_name']:35s} | {v_str}")
        results["rai_metrics_df"] = df

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Phase B: Recurrence Readiness
# ─────────────────────────────────────────────────────────────────────────────

RECURRENCE_READINESS_SQL = """
CREATE OR REPLACE TABLE val_recurrence_readiness_v1 AS
WITH base AS (
    SELECT
        r.research_id,
        r.recurrence_flag_structured,
        r.recurrence_any,
        r.first_recurrence_date,
        r.detection_category,
        r.recurrence_site_inferred,
        r.recurrence_data_confidence,
        r.n_recurrence_sources,
        r.tg_rising_flag,
        r.n_tg_measurements,
        COALESCE(r.recurrence_date_status, 'not_classified') AS date_status,
        COALESCE(r.recurrence_date_confidence, 0.0)          AS date_confidence,
        r.recurrence_date_best
    FROM extracted_recurrence_refined_v1 r
),
summary AS (
    SELECT
        'total_patients'                              AS category,
        COUNT(*)                                      AS n_patients,
        ROUND(100.0, 1)                               AS pct_of_cohort,
        NULL                                          AS date_quality_tier,
        NULL                                          AS analytically_usable,
        'Full cohort in extracted_recurrence_refined_v1' AS notes
    FROM base
    UNION ALL
    SELECT
        'recurrence_any_flagged',
        COUNT(*) FILTER (WHERE recurrence_any IS TRUE),
        ROUND(100.0 * COUNT(*) FILTER (WHERE recurrence_any IS TRUE) / COUNT(*), 1),
        NULL, NULL,
        'Any recurrence: structural OR biochemical (Tg rising > 2x nadir AND > 1.0)'
    FROM base
    UNION ALL
    SELECT
        'recurrence_structural_only',
        COUNT(*) FILTER (WHERE recurrence_flag_structured IS TRUE),
        ROUND(100.0 * COUNT(*) FILTER (WHERE recurrence_flag_structured IS TRUE) / COUNT(*), 1),
        NULL, NULL,
        'Structural recurrence from recurrence_risk_features_mv'
    FROM base
    UNION ALL
    SELECT
        'recurrence_biochemical_only',
        COUNT(*) FILTER (WHERE recurrence_any IS TRUE
                     AND recurrence_flag_structured IS NOT TRUE),
        ROUND(100.0 * COUNT(*) FILTER (WHERE recurrence_any IS TRUE
                     AND recurrence_flag_structured IS NOT TRUE) / COUNT(*), 1),
        NULL, NULL,
        'Biochemical-only recurrence (Tg trajectory without structural confirmation)'
    FROM base
    UNION ALL
    SELECT
        'date_exact_source',
        COUNT(*) FILTER (WHERE recurrence_any IS TRUE
                     AND date_status = 'exact_source_date'),
        ROUND(100.0 * COUNT(*) FILTER (WHERE recurrence_any IS TRUE
                     AND date_status = 'exact_source_date')
              / NULLIF(COUNT(*) FILTER (WHERE recurrence_any IS TRUE), 0), 1),
        'exact_source_date',
        'YES -- suitable for time-to-event analysis',
        'Day-level recurrence date from structured source'
    FROM base
    UNION ALL
    SELECT
        'date_biochem_inferred',
        COUNT(*) FILTER (WHERE recurrence_any IS TRUE
                     AND date_status = 'biochemical_inflection_inferred'),
        ROUND(100.0 * COUNT(*) FILTER (WHERE recurrence_any IS TRUE
                     AND date_status = 'biochemical_inflection_inferred')
              / NULLIF(COUNT(*) FILTER (WHERE recurrence_any IS TRUE), 0), 1),
        'biochemical_inflection_inferred',
        'CONDITIONAL -- usable for coarse survival analysis with caveat',
        'Tg inflection point date as recurrence proxy'
    FROM base
    UNION ALL
    SELECT
        'date_unresolved',
        COUNT(*) FILTER (WHERE recurrence_any IS TRUE
                     AND date_status = 'unresolved_date'),
        ROUND(100.0 * COUNT(*) FILTER (WHERE recurrence_any IS TRUE
                     AND date_status = 'unresolved_date')
              / NULLIF(COUNT(*) FILTER (WHERE recurrence_any IS TRUE), 0), 1),
        'unresolved_date',
        'NO -- flag-only; cannot be used in time-to-event models',
        'Recurrence flagged but no day-level date recoverable. Source limitation.'
    FROM base
    UNION ALL
    SELECT
        'source_linked_multi',
        COUNT(*) FILTER (WHERE recurrence_any IS TRUE AND n_recurrence_sources >= 2),
        ROUND(100.0 * COUNT(*) FILTER (WHERE recurrence_any IS TRUE AND n_recurrence_sources >= 2)
              / NULLIF(COUNT(*) FILTER (WHERE recurrence_any IS TRUE), 0), 1),
        NULL,
        'HIGH CONFIDENCE -- multi-source corroboration',
        'Recurrence corroborated by 2+ independent data sources'
    FROM base
    UNION ALL
    SELECT
        'site_identified',
        COUNT(*) FILTER (WHERE recurrence_any IS TRUE
                     AND recurrence_site_inferred IS NOT NULL),
        ROUND(100.0 * COUNT(*) FILTER (WHERE recurrence_any IS TRUE
                     AND recurrence_site_inferred IS NOT NULL)
              / NULLIF(COUNT(*) FILTER (WHERE recurrence_any IS TRUE), 0), 1),
        NULL, NULL,
        'Recurrence with anatomic site inferred from scan findings'
    FROM base
    UNION ALL
    SELECT
        'tg_trajectory_available',
        COUNT(*) FILTER (WHERE recurrence_any IS TRUE AND n_tg_measurements >= 3),
        ROUND(100.0 * COUNT(*) FILTER (WHERE recurrence_any IS TRUE AND n_tg_measurements >= 3)
              / NULLIF(COUNT(*) FILTER (WHERE recurrence_any IS TRUE), 0), 1),
        NULL, NULL,
        'Recurrence patients with >= 3 Tg measurements for trajectory analysis'
    FROM base
)
SELECT
    s.category,
    CAST(s.n_patients AS INTEGER) AS n_patients,
    s.pct_of_cohort,
    s.date_quality_tier,
    s.analytically_usable,
    s.notes,
    CURRENT_TIMESTAMP AS audited_at
FROM summary s
"""


def phase_b(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase B: Recurrence Readiness Hardening")
    results: dict = {}

    if not table_exists(con, "extracted_recurrence_refined_v1"):
        print("  [SKIP] extracted_recurrence_refined_v1 not found")
        return results

    print("  Creating val_recurrence_readiness_v1...")
    safe_exec(con, RECURRENCE_READINESS_SQL, dry)
    ct = safe_count(con, "SELECT COUNT(*) FROM val_recurrence_readiness_v1")
    print(f"  -> {ct} readiness rows")
    results["recurrence_readiness_rows"] = ct

    if not dry:
        df = con.execute("SELECT category, n_patients, pct_of_cohort, "
                         "date_quality_tier, analytically_usable "
                         "FROM val_recurrence_readiness_v1 "
                         "ORDER BY n_patients DESC").fetchdf()
        for _, row in df.iterrows():
            n = row["n_patients"]
            pct = row["pct_of_cohort"]
            tier = row["date_quality_tier"] or ""
            usable = row["analytically_usable"] or ""
            print(f"    {row['category']:35s} | {n:>7,} | {pct:>5.1f}% | {tier:30s} | {usable}")
        results["recurrence_df"] = df

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Phase C: Non-Tg Lab Temporal Truth
# ─────────────────────────────────────────────────────────────────────────────

LAB_TEMPORAL_TRUTH_SQL = """
CREATE OR REPLACE TABLE val_lab_temporal_truth_v1 AS
WITH canonical AS (
    SELECT
        lab_name_standardized,
        analyte_group,
        data_completeness_tier,
        COUNT(*)                                                   AS n_values,
        COUNT(DISTINCT research_id)                                AS n_patients,
        COUNT(*) FILTER (WHERE lab_date IS NOT NULL)               AS n_with_date,
        COUNT(*) FILTER (WHERE lab_date_status = 'exact_collection_date')
                                                                   AS n_exact_collection_date,
        COUNT(*) FILTER (WHERE lab_date_status = 'extracted_date') AS n_extracted_date,
        COUNT(*) FILTER (WHERE lab_date_status = 'unresolved_date'
                            OR lab_date IS NULL)                    AS n_no_date,
        COUNT(*) FILTER (WHERE value_numeric IS NOT NULL)          AS n_numeric_parsed,
        COUNT(*) FILTER (WHERE is_censored IS TRUE)                AS n_censored,
        MIN(lab_date)                                              AS earliest_date,
        MAX(lab_date)                                              AS latest_date
    FROM longitudinal_lab_canonical_v1
    GROUP BY lab_name_standardized, analyte_group, data_completeness_tier
),
future_placeholders AS (
    SELECT
        lab_name_standardized,
        analyte_group,
        data_completeness_tier,
        0 AS n_values, 0 AS n_patients, 0 AS n_with_date,
        0 AS n_exact_collection_date, 0 AS n_extracted_date, 0 AS n_no_date,
        0 AS n_numeric_parsed, 0 AS n_censored,
        NULL AS earliest_date, NULL AS latest_date
    FROM val_lab_completeness_v1
    WHERE n_measurements = 0
      AND lab_name_standardized NOT IN (SELECT lab_name_standardized FROM canonical)
)
SELECT
    c.lab_name_standardized,
    c.analyte_group,
    c.data_completeness_tier,
    c.n_values,
    c.n_patients,
    c.n_with_date,
    ROUND(100.0 * c.n_with_date / NULLIF(c.n_values, 0), 1) AS date_coverage_pct,
    c.n_exact_collection_date,
    c.n_extracted_date,
    c.n_no_date,
    c.n_numeric_parsed,
    c.n_censored,
    c.earliest_date,
    c.latest_date,
    CASE
        WHEN c.data_completeness_tier = 'current_structured'
         AND c.n_exact_collection_date > 0
        THEN 'FULL -- structured collection date from lab system'
        WHEN c.data_completeness_tier = 'current_nlp_partial'
         AND c.n_extracted_date > 0
        THEN 'PARTIAL -- NLP-extracted or note-anchored date only'
        WHEN c.data_completeness_tier = 'future_institutional_required'
        THEN 'NONE -- no data available; awaiting institutional lab feed'
        WHEN c.n_no_date = c.n_values
        THEN 'NONE -- values present but all dates unresolved'
        ELSE 'MIXED -- combination of date qualities'
    END AS date_source_type,
    CASE
        WHEN c.data_completeness_tier = 'current_structured'
         AND c.n_exact_collection_date > 0
        THEN 'time_to_event_eligible'
        WHEN c.data_completeness_tier = 'current_nlp_partial'
         AND c.n_extracted_date > c.n_values * 0.5
        THEN 'postop_window_eligible_with_caveat'
        WHEN c.n_values > 0 AND c.n_no_date = c.n_values
        THEN 'value_only_no_temporal'
        WHEN c.n_values = 0
        THEN 'no_data_source_absent'
        ELSE 'limited_temporal_fidelity'
    END AS analysis_suitability,
    CASE
        WHEN c.data_completeness_tier = 'current_structured'
        THEN 'Structured lab table with specimen_collect_dt. '
             'Safe for time-to-event and postoperative day analysis.'
        WHEN c.data_completeness_tier = 'current_nlp_partial'
        THEN 'NLP-extracted from clinical notes. Date is note_date or nearby-date extraction. '
             'NOT suitable for precise postoperative-day analysis. '
             'Acceptable for broad temporal windowing (e.g. 0-30d vs 31-180d).'
        WHEN c.data_completeness_tier = 'future_institutional_required'
        THEN 'Zero measurements in database. Source limitation: institutional lab feed '
             'not available. Cannot derive values from any existing data source.'
        ELSE 'Mixed provenance. Review per-measurement date_status before use.'
    END AS temporal_fidelity_note,
    CASE
        WHEN c.lab_name_standardized IN ('thyroglobulin', 'anti_thyroglobulin')
        THEN 'Lab system structured table with specimen_collect_dt'
        WHEN c.lab_name_standardized IN ('pth', 'calcium_total', 'calcium_ionized')
        THEN 'NLP extraction from clinical notes + extracted_clinical_events_v4'
        ELSE 'No structured source; future institutional lab feed required'
    END AS primary_source_description,
    CURRENT_TIMESTAMP AS audited_at
FROM (SELECT * FROM canonical UNION ALL SELECT * FROM future_placeholders) c
ORDER BY
    CASE c.data_completeness_tier
        WHEN 'current_structured' THEN 1
        WHEN 'current_nlp_partial' THEN 2
        WHEN 'future_institutional_required' THEN 3
    END,
    c.n_values DESC
"""


def phase_c(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase C: Non-Tg Lab Temporal Truth")
    results: dict = {}

    if not table_exists(con, "longitudinal_lab_canonical_v1"):
        print("  [SKIP] longitudinal_lab_canonical_v1 not found")
        return results

    print("  Creating val_lab_temporal_truth_v1...")
    safe_exec(con, LAB_TEMPORAL_TRUTH_SQL, dry)
    ct = safe_count(con, "SELECT COUNT(*) FROM val_lab_temporal_truth_v1")
    print(f"  -> {ct} lab temporal truth rows")
    results["lab_truth_rows"] = ct

    if not dry:
        df = con.execute("SELECT lab_name_standardized, n_values, n_patients, "
                         "date_coverage_pct, date_source_type, analysis_suitability "
                         "FROM val_lab_temporal_truth_v1 "
                         "ORDER BY n_values DESC").fetchdf()
        for _, row in df.iterrows():
            nv = row["n_values"]
            np_ = row["n_patients"]
            dp = row["date_coverage_pct"]
            dp_str = f"{dp:.1f}%" if pd.notna(dp) else "N/A"
            print(f"    {row['lab_name_standardized']:25s} | {nv:>8,} vals | "
                  f"{np_:>6,} pts | date: {dp_str:>6s} | "
                  f"{row['analysis_suitability']}")
        results["lab_df"] = df

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Phase D: Operative Field Semantics
# ─────────────────────────────────────────────────────────────────────────────

OPERATIVE_SEMANTICS_SQL = """
CREATE OR REPLACE TABLE val_operative_field_semantics_v1 AS
WITH op_notes AS (
    SELECT DISTINCT research_id
    FROM clinical_notes_long
    WHERE note_type = 'op_note'
),
field_audit AS (
    SELECT
        oe.research_id,
        oe.surgery_episode_id,
        (on2.research_id IS NOT NULL) AS has_op_note,
        oe.rln_monitoring_flag,
        oe.parathyroid_autograft_flag,
        oe.parathyroid_resection_flag,
        oe.gross_ete_flag,
        oe.local_invasion_flag,
        oe.tracheal_involvement_flag,
        oe.esophageal_involvement_flag,
        oe.strap_muscle_involvement_flag,
        oe.reoperative_field_flag,
        oe.drain_flag,
        oe.central_neck_dissection_flag,
        oe.lateral_neck_dissection_flag,
        oe.berry_ligament_flag,
        oe.frozen_section_flag,
        oe.ebl_ml,
        oe.ebl_ml_nlp,
        oe.parathyroid_identified_count,
        oe.op_enrichment_source
    FROM operative_episode_detail_v2 oe
    LEFT JOIN op_notes on2 ON oe.research_id = on2.research_id
)
SELECT
    UNNEST([
        'rln_monitoring_flag',
        'parathyroid_autograft_flag',
        'parathyroid_resection_flag',
        'gross_ete_flag',
        'local_invasion_flag',
        'tracheal_involvement_flag',
        'esophageal_involvement_flag',
        'strap_muscle_involvement_flag',
        'reoperative_field_flag',
        'drain_flag',
        'central_neck_dissection_flag',
        'lateral_neck_dissection_flag',
        'berry_ligament_flag',
        'frozen_section_flag',
        'ebl_ml (structured)',
        'ebl_ml_nlp',
        'parathyroid_identified_count'
    ]) AS field_name,
    UNNEST([
        COUNT(*) FILTER (WHERE rln_monitoring_flag IS TRUE),
        COUNT(*) FILTER (WHERE parathyroid_autograft_flag IS TRUE),
        COUNT(*) FILTER (WHERE parathyroid_resection_flag IS TRUE),
        COUNT(*) FILTER (WHERE gross_ete_flag IS TRUE),
        COUNT(*) FILTER (WHERE local_invasion_flag IS TRUE),
        COUNT(*) FILTER (WHERE tracheal_involvement_flag IS TRUE),
        COUNT(*) FILTER (WHERE esophageal_involvement_flag IS TRUE),
        COUNT(*) FILTER (WHERE strap_muscle_involvement_flag IS TRUE),
        COUNT(*) FILTER (WHERE reoperative_field_flag IS TRUE),
        COUNT(*) FILTER (WHERE drain_flag IS TRUE),
        COUNT(*) FILTER (WHERE central_neck_dissection_flag IS TRUE),
        COUNT(*) FILTER (WHERE lateral_neck_dissection_flag IS TRUE),
        COUNT(*) FILTER (WHERE berry_ligament_flag IS TRUE),
        COUNT(*) FILTER (WHERE frozen_section_flag IS TRUE),
        COUNT(*) FILTER (WHERE ebl_ml IS NOT NULL),
        COUNT(*) FILTER (WHERE ebl_ml_nlp IS NOT NULL),
        COUNT(*) FILTER (WHERE parathyroid_identified_count IS NOT NULL)
    ]) AS n_true_or_present,
    UNNEST([
        COUNT(*) FILTER (WHERE rln_monitoring_flag IS FALSE),
        COUNT(*) FILTER (WHERE parathyroid_autograft_flag IS FALSE),
        COUNT(*) FILTER (WHERE parathyroid_resection_flag IS FALSE),
        COUNT(*) FILTER (WHERE gross_ete_flag IS FALSE),
        COUNT(*) FILTER (WHERE local_invasion_flag IS FALSE),
        COUNT(*) FILTER (WHERE tracheal_involvement_flag IS FALSE),
        COUNT(*) FILTER (WHERE esophageal_involvement_flag IS FALSE),
        COUNT(*) FILTER (WHERE strap_muscle_involvement_flag IS FALSE),
        COUNT(*) FILTER (WHERE reoperative_field_flag IS FALSE),
        COUNT(*) FILTER (WHERE drain_flag IS FALSE),
        COUNT(*) FILTER (WHERE central_neck_dissection_flag IS FALSE),
        COUNT(*) FILTER (WHERE lateral_neck_dissection_flag IS FALSE),
        COUNT(*) FILTER (WHERE berry_ligament_flag IS FALSE),
        COUNT(*) FILTER (WHERE frozen_section_flag IS FALSE),
        0, 0, 0
    ]) AS n_false,
    UNNEST([
        COUNT(*) FILTER (WHERE rln_monitoring_flag IS NULL),
        COUNT(*) FILTER (WHERE parathyroid_autograft_flag IS NULL),
        COUNT(*) FILTER (WHERE parathyroid_resection_flag IS NULL),
        COUNT(*) FILTER (WHERE gross_ete_flag IS NULL),
        COUNT(*) FILTER (WHERE local_invasion_flag IS NULL),
        COUNT(*) FILTER (WHERE tracheal_involvement_flag IS NULL),
        COUNT(*) FILTER (WHERE esophageal_involvement_flag IS NULL),
        COUNT(*) FILTER (WHERE strap_muscle_involvement_flag IS NULL),
        COUNT(*) FILTER (WHERE reoperative_field_flag IS NULL),
        COUNT(*) FILTER (WHERE drain_flag IS NULL),
        COUNT(*) FILTER (WHERE central_neck_dissection_flag IS NULL),
        COUNT(*) FILTER (WHERE lateral_neck_dissection_flag IS NULL),
        COUNT(*) FILTER (WHERE berry_ligament_flag IS NULL),
        COUNT(*) FILTER (WHERE frozen_section_flag IS NULL),
        COUNT(*) FILTER (WHERE ebl_ml IS NULL),
        COUNT(*) FILTER (WHERE ebl_ml_nlp IS NULL),
        COUNT(*) FILTER (WHERE parathyroid_identified_count IS NULL)
    ]) AS n_null,
    COUNT(*) AS total_episodes,
    COUNT(*) FILTER (WHERE has_op_note) AS episodes_with_op_note,
    UNNEST([
        'V2_EXTRACTOR_NOT_MATERIALIZED',
        'V2_EXTRACTOR_NOT_MATERIALIZED',
        'V2_EXTRACTOR_NOT_MATERIALIZED',
        'V2_EXTRACTOR_NOT_MATERIALIZED',
        'V2_EXTRACTOR_NOT_MATERIALIZED',
        'V2_EXTRACTOR_NOT_MATERIALIZED',
        'V2_EXTRACTOR_NOT_MATERIALIZED',
        'V2_EXTRACTOR_NOT_MATERIALIZED',
        'V2_EXTRACTOR_NOT_MATERIALIZED',
        'V2_EXTRACTOR_NOT_MATERIALIZED',
        'STRUCTURED_DATA_SOURCE',
        'STRUCTURED_DATA_SOURCE',
        'NLP_ENTITY_TYPE_NOT_IN_VOCABULARY',
        'NLP_ENTITY_TYPE_NOT_IN_VOCABULARY',
        'STRUCTURED_DATA_PARTIAL',
        'NLP_ENTITY_TYPE_NOT_IN_VOCABULARY',
        'NLP_ENTITY_TYPE_NOT_IN_VOCABULARY'
    ]) AS data_source_status,
    UNNEST([
        'NOT_PARSED -- all FALSE values are hardcoded defaults from script 22; '
        'V2 OperativeDetailExtractor exists but output not materialized to note_entities_procedures',
        'NOT_PARSED -- same as rln_monitoring_flag',
        'NOT_PARSED -- same; script 76 ALTER TABLE column also NULL (no NLP entity source)',
        'NOT_PARSED -- same as rln_monitoring_flag',
        'NOT_PARSED -- same as rln_monitoring_flag',
        'NOT_PARSED -- same as rln_monitoring_flag',
        'NOT_PARSED -- same as rln_monitoring_flag',
        'NOT_PARSED -- same as rln_monitoring_flag',
        'NOT_PARSED -- same as rln_monitoring_flag',
        'NOT_PARSED -- same as rln_monitoring_flag',
        'RELIABLE -- derived from path_synoptics structured fields',
        'RELIABLE -- derived from path_synoptics + extracted_lateral_neck_v1',
        'SOURCE_ABSENT -- berry_ligament entity type not in NLP vocabulary; '
        'V2 extractor has pattern bank but output not materialized',
        'SOURCE_ABSENT -- frozen_section entity type not in NLP vocabulary; same gap',
        'PARTIAL -- ebl_ml from operative_details.ebl structured field; non-NULL values are reliable',
        'SOURCE_ABSENT -- ebl entity type not in NLP vocabulary; same gap as berry_ligament',
        'SOURCE_ABSENT -- parathyroid_count_identified not in NLP vocabulary'
    ]) AS semantic_truth_label,
    UNNEST([
        'NOT_PARSED', 'NOT_PARSED', 'NOT_PARSED',
        'NOT_PARSED', 'NOT_PARSED', 'NOT_PARSED',
        'NOT_PARSED', 'NOT_PARSED', 'NOT_PARSED', 'NOT_PARSED',
        'RELIABLE', 'RELIABLE',
        'SOURCE_ABSENT', 'SOURCE_ABSENT',
        'PARTIAL', 'SOURCE_ABSENT', 'SOURCE_ABSENT'
    ]) AS parse_status,
    CURRENT_TIMESTAMP AS audited_at
FROM field_audit
"""


def phase_d(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase D: Operative Field Semantics Hardening")
    results: dict = {}

    if not table_exists(con, "operative_episode_detail_v2"):
        print("  [SKIP] operative_episode_detail_v2 not found")
        return results

    ep_ct = safe_count(con, "SELECT COUNT(*) FROM operative_episode_detail_v2")
    print(f"  Operative episodes: {ep_ct}")

    print("  Creating val_operative_field_semantics_v1...")
    safe_exec(con, OPERATIVE_SEMANTICS_SQL, dry)
    ct = safe_count(con, "SELECT COUNT(*) FROM val_operative_field_semantics_v1")
    print(f"  -> {ct} field semantics rows")
    results["operative_fields"] = ct

    if not dry:
        df = con.execute("SELECT field_name, n_true_or_present, n_false, n_null, "
                         "parse_status, data_source_status "
                         "FROM val_operative_field_semantics_v1 "
                         "ORDER BY parse_status, field_name").fetchdf()
        for _, row in df.iterrows():
            print(f"    {row['field_name']:35s} | TRUE={row['n_true_or_present']:>6,} | "
                  f"FALSE={row['n_false']:>6,} | NULL={row['n_null']:>6,} | "
                  f"{row['parse_status']}")
        results["operative_df"] = df

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Phase E: Documentation + Exports
# ─────────────────────────────────────────────────────────────────────────────

def generate_rai_doc(results: dict) -> str:
    lines = [
        f"# RAI Structural Gap Maximization Audit — {DATE_STAMP}",
        "",
        "## Summary",
        "",
        "This document captures the end-state of RAI data coverage after all extraction,",
        "refinement, and hardening phases. It identifies source limitations that cannot",
        "be resolved without additional institutional data feeds.",
        "",
        "## Source Limitation: Nuclear Medicine Reports",
        "",
        "**Status: FIRST-CLASS STRUCTURAL LIMITATION**",
        "",
        "Zero nuclear medicine notes exist in the `clinical_notes_long` corpus.",
        "RAI dose/scan data relies entirely on endocrine clinic notes and discharge",
        "summaries. This constrains dose coverage to approximately 41% of confirmed",
        "RAI episodes. This limitation is encoded in `val_rai_source_limitation_v1`",
        "as `nuclear_medicine_reports_absent`.",
        "",
        "## Coverage Metrics",
        "",
        "All metrics are sourced from `val_rai_structural_coverage_v1`.",
        "",
    ]
    if "rai_metrics_df" in results:
        df = results["rai_metrics_df"]
        lines.append("| Metric | Value | Category |")
        lines.append("|--------|-------|----------|")
        for _, r in df.iterrows():
            v = r["metric_value"]
            v_str = f"{int(v):,}" if v == int(v) else f"{v:.1f}"
            lines.append(f"| {r['metric_name']} | {v_str} | {r['metric_category']} |")
        lines.append("")

    lines += [
        "## RAI Assertion Status Taxonomy",
        "",
        "| Status | Meaning | Manuscript Use |",
        "|--------|---------|----------------|",
        "| definite_received | Explicit documentation of RAI administration | Primary analysis |",
        "| likely_received | Strong evidence but not explicitly stated | Primary with caveat |",
        "| planned | RAI planned but completion not confirmed | Exclude from primary |",
        "| historical | Reference to prior RAI in older notes | Exclude unless corroborated |",
        "| negated | RAI explicitly not given / refused | Negative control |",
        "| ambiguous | Unclear whether RAI was administered | Manual review queue |",
        "",
        "## Dose Missingness Classification",
        "",
        "| Category | Description |",
        "|----------|-------------|",
        "| dose_available | Dose value present in episode record |",
        "| source_present_no_dose_stated | Endocrine/DC note exists but dose not mentioned |",
        "| linkage_failed | NLP dose found but could not link to RAI episode |",
        "| no_source_report_available | No endocrine, DC, or nuclear med notes for patient |",
        "",
        "## Source Domain Availability",
        "",
        "| Source | Status | Impact |",
        "|--------|--------|--------|",
        "| Nuclear medicine reports | ABSENT | Cannot improve dose coverage beyond ~41% |",
        "| Endocrine clinic notes | PARTIAL | Primary RAI data source; coverage patient-dependent |",
        "| Structured RAI orders | ABSENT | No pharmacy/order system integration |",
        "| Discharge summaries | PARTIAL (1.6%) | Low yield for RAI-specific data |",
        "",
        "## Validation Tables Created",
        "",
        "- `val_rai_structural_coverage_v1` — 27 coverage metrics",
        "- `val_rai_source_limitation_v1` — 5 source domain limitation entries",
        "",
        f"Generated: {datetime.now().isoformat()}",
    ]
    return "\n".join(lines)


def generate_recurrence_doc(results: dict) -> str:
    lines = [
        f"# Recurrence Structural Gap Maximization Audit — {DATE_STAMP}",
        "",
        "## Summary",
        "",
        "This document captures the recurrence date-quality distribution and",
        "analytically-usable subset. The primary finding is that 88.8% of",
        "recurrence cases have unresolved dates — a source limitation, not a",
        "pipeline gap. Structural recurrence flags are derived from",
        "`recurrence_risk_features_mv`; biochemical recurrence from Tg trajectory.",
        "",
        "## Three Recurrence Concepts",
        "",
        "| Concept | Definition | Manuscript Use |",
        "|---------|------------|----------------|",
        "| Structurally flagged | `recurrence_any = TRUE` in extracted_recurrence_refined_v1 | Prevalence reporting |",
        "| Date-bearing | flagged AND `recurrence_date_status != 'unresolved_date'` | Time-to-event eligible |",
        "| Analytically usable | date-bearing AND multi-source corroboration | Primary survival analysis |",
        "",
        "## Date Quality Taxonomy",
        "",
        "| Tier | Confidence | Description | Analysis Suitability |",
        "|------|------------|-------------|---------------------|",
        "| exact_source_date | 1.0 | Day-level date from structured source | Full time-to-event |",
        "| biochemical_inflection_inferred | 0.5 | Tg inflection point as proxy | Coarse survival with caveat |",
        "| unresolved_date | 0.0 | Flag only, no recoverable date | Flag-only; exclude from TTE |",
        "| not_applicable | N/A | No recurrence flagged | N/A |",
        "",
        "## Readiness Table",
        "",
    ]
    if "recurrence_df" in results:
        df = results["recurrence_df"]
        lines.append("| Category | N | % | Date Tier | Usable |")
        lines.append("|----------|---|---|-----------|--------|")
        for _, r in df.iterrows():
            tier = r["date_quality_tier"] if pd.notna(r["date_quality_tier"]) else ""
            usable = r["analytically_usable"] if pd.notna(r["analytically_usable"]) else ""
            pct = r["pct_of_cohort"]
            pct_str = f"{pct:.1f}" if pd.notna(pct) else "N/A"
            lines.append(f"| {r['category']} | {r['n_patients']:,} | {pct_str}% | {tier} | {usable} |")
        lines.append("")

    lines += [
        "## Source Limitations",
        "",
        "The 88.8% unresolved-date rate reflects that most recurrence data comes from",
        "`recurrence_risk_features_mv.recurrence_flag` which is a boolean flag without",
        "a day-level date. The underlying `extracted_clinical_events_v3` NLP extraction",
        "tagged events as 'recurrence' but frequently from H&P notes that describe",
        "history rather than incident events. Without a structured recurrence registry",
        "(e.g., cancer registry follow-up data), day-level dates cannot be recovered.",
        "",
        "## Validation Tables Created",
        "",
        "- `val_recurrence_readiness_v1` — manuscript readiness summary (10 rows)",
        "",
        f"Generated: {datetime.now().isoformat()}",
    ]
    return "\n".join(lines)


def generate_lab_doc(results: dict) -> str:
    lines = [
        f"# Non-Tg Lab Temporal Truth Audit — {DATE_STAMP}",
        "",
        "## Summary",
        "",
        "This document formalizes the temporal fidelity of each lab analyte in the",
        "database. Thyroglobulin and anti-thyroglobulin have structured collection",
        "dates from the lab system. All other analytes (PTH, calcium, TSH, etc.)",
        "either have NLP-extracted dates or no data at all.",
        "",
        "## Analysis Suitability Classification",
        "",
        "| Tier | Meaning | Example Analytes |",
        "|------|---------|------------------|",
        "| time_to_event_eligible | Structured collection date; safe for postop-day analysis | thyroglobulin, anti_tg |",
        "| postop_window_eligible_with_caveat | NLP date; acceptable for broad windows (0-30d, 31-180d) | pth, calcium_total |",
        "| value_only_no_temporal | Values exist but dates are unreliable | (none currently) |",
        "| no_data_source_absent | Zero measurements; institutional feed required | tsh, free_t4, vitamin_d |",
        "| limited_temporal_fidelity | Mixed provenance; review per-measurement | calcium_ionized |",
        "",
        "## Per-Analyte Coverage",
        "",
    ]
    if "lab_df" in results:
        df = results["lab_df"]
        lines.append("| Analyte | Values | Patients | Date Coverage | Suitability |")
        lines.append("|---------|--------|----------|---------------|-------------|")
        for _, r in df.iterrows():
            dp = r["date_coverage_pct"]
            dp_str = f"{dp:.1f}%" if pd.notna(dp) else "N/A"
            lines.append(f"| {r['lab_name_standardized']} | {r['n_values']:,} | "
                         f"{r['n_patients']:,} | {dp_str} | {r['analysis_suitability']} |")
        lines.append("")

    lines += [
        "## Critical Notes for Manuscript Authors",
        "",
        "1. **Do NOT report PTH/calcium values as 'postoperative day X'** unless the",
        "   `lab_date_status` is `exact_collection_date`. NLP-extracted dates have",
        "   note-level granularity, not specimen-collection granularity.",
        "",
        "2. **TSH, free T4/T3, vitamin D, albumin** have zero measurements. These",
        "   are formally classified as `future_institutional_required`. The dashboard",
        "   and manuscript should NOT imply these labs are available.",
        "",
        "3. **Censored values** (e.g., '<0.2') are flagged with `is_censored = TRUE`.",
        "   Use appropriate methods (e.g., Kaplan-Meier for censored Tg).",
        "",
        "## Validation Tables Created",
        "",
        "- `val_lab_temporal_truth_v1` — per-analyte temporal truth audit",
        "",
        f"Generated: {datetime.now().isoformat()}",
    ]
    return "\n".join(lines)


def generate_operative_doc(results: dict) -> str:
    lines = [
        f"# Operative Semantics Hardening Audit — {DATE_STAMP}",
        "",
        "## Summary",
        "",
        "This document audits the semantic truth of boolean operative fields in",
        "`operative_episode_detail_v2`. The core problem: 10 boolean fields are",
        "hardcoded to FALSE in script 22, but this represents 'NOT_PARSED' rather",
        "than 'confirmed negative'. The V2 OperativeDetailExtractor exists and CAN",
        "extract these fields, but its output was never materialized to the",
        "`note_entities_procedures` table that script 76 reads from.",
        "",
        "## Parse Status Taxonomy",
        "",
        "| Status | Meaning | Boolean Interpretation |",
        "|--------|---------|----------------------|",
        "| RELIABLE | Derived from structured data (path_synoptics) | TRUE/FALSE are accurate |",
        "| PARTIAL | Structured source covers subset of patients | Non-NULL values are accurate |",
        "| NOT_PARSED | V2 extractor exists but output not materialized | FALSE = UNKNOWN (not confirmed negative) |",
        "| SOURCE_ABSENT | NLP entity type not in vocabulary | NULL = no extraction attempted |",
        "",
        "## Per-Field Audit",
        "",
    ]
    if "operative_df" in results:
        df = results["operative_df"]
        lines.append("| Field | TRUE | FALSE | NULL | Parse Status | Source |")
        lines.append("|-------|------|-------|------|--------------|--------|")
        for _, r in df.iterrows():
            lines.append(f"| {r['field_name']} | {r['n_true_or_present']:,} | "
                         f"{r['n_false']:,} | {r['n_null']:,} | "
                         f"{r['parse_status']} | {r['data_source_status']} |")
        lines.append("")

    lines += [
        "## Architecture Gap: V2 Extractor Materialization",
        "",
        "The `OperativeDetailExtractor` in `notes_extraction/extract_operative_v2.py`",
        "has 13 domain pattern banks that CAN extract berry_ligament, frozen_section,",
        "EBL, parathyroid management, and more. However:",
        "",
        "1. Script 22 creates operative_episode_detail_v2 with hardcoded FALSE",
        "2. Script 22 runs V2 extractors inline but via COALESCE(new, old)",
        "3. Since old = FALSE (non-NULL), COALESCE never overwrites",
        "4. Script 76 adds ALTER TABLE columns (NULL) and tries to UPDATE from",
        "   note_entities_procedures, but the V2 entity types are not in that table",
        "",
        "**Remediation**: Run V2 extractors to a staging table, then UPDATE",
        "operative_episode_detail_v2 using IS NULL OR original_value = FALSE guard.",
        "This is a future pipeline improvement, not a current data quality issue.",
        "",
        "## Recommendation for Manuscript Use",
        "",
        "- **central_neck_dissection_flag**: SAFE to use (structured source)",
        "- **lateral_neck_dissection_flag**: SAFE to use (structured + Phase 10 NLP)",
        "- **ebl_ml**: SAFE where non-NULL (structured operative_details.ebl)",
        "- **All other boolean fields**: Treat FALSE as UNKNOWN in analyses.",
        "  Do NOT report 'X% had RLN monitoring' based on rln_monitoring_flag.",
        "",
        "## Validation Tables Created",
        "",
        "- `val_operative_field_semantics_v1` — per-field parse status audit",
        "",
        f"Generated: {datetime.now().isoformat()}",
    ]
    return "\n".join(lines)


def phase_e(con: duckdb.DuckDBPyConnection, dry: bool,
            all_results: dict) -> dict:
    section("Phase E: Documentation + Exports")
    results: dict = {}
    docs_dir = ROOT / "docs"
    docs_dir.mkdir(exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    doc_map = {
        f"rai_structural_gap_maximization_{DATE_STAMP}.md":
            generate_rai_doc(all_results.get("phase_a", {})),
        f"recurrence_structural_gap_maximization_{DATE_STAMP}.md":
            generate_recurrence_doc(all_results.get("phase_b", {})),
        f"non_tg_lab_temporal_truth_audit_{DATE_STAMP}.md":
            generate_lab_doc(all_results.get("phase_c", {})),
        f"operative_semantics_hardening_{DATE_STAMP}.md":
            generate_operative_doc(all_results.get("phase_d", {})),
    }

    for fname, content in doc_map.items():
        path = docs_dir / fname
        if not dry:
            path.write_text(content, encoding="utf-8")
            print(f"  Wrote: {path}")
        else:
            print(f"  [DRY RUN] Would write: {path}")
        results[fname] = str(path)

    val_tables = [
        "val_rai_structural_coverage_v1",
        "val_rai_source_limitation_v1",
        "val_recurrence_readiness_v1",
        "val_lab_temporal_truth_v1",
        "val_operative_field_semantics_v1",
    ]
    for tbl in val_tables:
        if not dry and table_exists(con, tbl):
            csv_path = EXPORT_DIR / f"{tbl}.csv"
            df = con.execute(f"SELECT * FROM {tbl}").fetchdf()
            df.to_csv(csv_path, index=False)
            print(f"  Exported: {csv_path}")
            results[f"export_{tbl}"] = len(df)

    manifest = {
        "script": "80_structural_gap_maximization",
        "timestamp": TIMESTAMP,
        "phases": ["A_rai", "B_recurrence", "C_lab", "D_operative", "E_docs"],
        "validation_tables_created": val_tables,
        "docs_created": list(doc_map.keys()),
        "export_dir": str(EXPORT_DIR),
    }
    if not dry:
        mpath = EXPORT_DIR / "manifest.json"
        mpath.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
        print(f"  Wrote: {mpath}")

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Structural gap maximization pass")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Dry run")
    parser.add_argument("--phase", default="all",
                        help="Phase to run: A/B/C/D/E/all")
    args = parser.parse_args()

    print(f"\n{'#' * 72}")
    print("  Script 80: Structural Gap Maximization Pass")
    print(f"  Target: {'MotherDuck' if args.md else 'Local DuckDB'}")
    print(f"  Dry run: {args.dry_run}")
    print(f"  Phase: {args.phase}")
    print(f"  Timestamp: {TIMESTAMP}")
    print(f"{'#' * 72}\n")

    con = connect(args)
    all_results: dict = {}
    phases = args.phase.upper()

    if phases in ("A", "ALL"):
        all_results["phase_a"] = phase_a(con, args.dry_run)

    if phases in ("B", "ALL"):
        all_results["phase_b"] = phase_b(con, args.dry_run)

    if phases in ("C", "ALL"):
        all_results["phase_c"] = phase_c(con, args.dry_run)

    if phases in ("D", "ALL"):
        all_results["phase_d"] = phase_d(con, args.dry_run)

    if phases in ("E", "ALL"):
        all_results["phase_e"] = phase_e(con, args.dry_run, all_results)

    section("COMPLETE")
    for phase_name, phase_results in all_results.items():
        print(f"  {phase_name}:")
        for k, v in phase_results.items():
            if not isinstance(v, pd.DataFrame):
                print(f"    {k}: {v}")

    con.close()
    print(f"\nDone. Exports in: {EXPORT_DIR}")


if __name__ == "__main__":
    main()
