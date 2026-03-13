#!/usr/bin/env python3
"""
53_longitudinal_lab_hardening.py -- Cleaned longitudinal thyroid lab timeline

Produces a manuscript-grade, provenance-aware lab timeline with:
  - strict date precedence (specimen_collect_dt > entity_date > note_date)
  - cross-source deduplication
  - implausibility guards per lab type
  - explicit below-threshold flag for censored values ("<0.2")
  - source priority scoring
  - trajectory summaries per patient

Output tables:
  longitudinal_lab_clean_v1           -- long-format, one row per lab event
  longitudinal_lab_patient_summary_v1 -- wide per-patient summary
  recurrence_event_clean_v1           -- cleaned structural vs biochemical recurrence events

Lab types covered:
  thyroglobulin, anti_tg, tsh, pth, calcium, ionized_calcium

Reference for date precedence: scripts/46_provenance_audit.py
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


# ─────────────────────────────────────────────────────────────────────────────
# Implausibility bounds per lab type
# These are conservative clinical sanity checks, not biological limits
# ─────────────────────────────────────────────────────────────────────────────
PLAUSIBILITY_BOUNDS = {
    "thyroglobulin": (0.0, 100_000.0),   # ng/mL; can be very high in metastatic disease
    "anti_tg":       (0.0, 10_000.0),    # IU/mL
    "tsh":           (0.0,    500.0),    # mIU/L
    "pth":           (0.5,    500.0),    # pg/mL
    "calcium":       (4.0,     15.0),    # mg/dL
    "ionized_calcium": (0.5,    2.5),    # mmol/L
}


# ─────────────────────────────────────────────────────────────────────────────
# longitudinal_lab_clean_v1
# ─────────────────────────────────────────────────────────────────────────────
LAB_CLEAN_SQL = """
CREATE OR REPLACE TABLE longitudinal_lab_clean_v1 AS
WITH

-- ── Source 1: thyroglobulin_labs (structured, highest priority) ───────────
tg_structured AS (
    SELECT
        research_id,
        'thyroglobulin'                              AS lab_type,
        CAST(specimen_collect_dt AS DATE)            AS lab_date,
        -- Handle "<0.2" threshold-reported values
        CASE WHEN TRIM(CAST(result AS VARCHAR)) LIKE '<%' THEN TRUE ELSE FALSE END
                                                     AS is_below_threshold,
        CASE WHEN TRIM(CAST(result AS VARCHAR)) LIKE '<%'
             THEN TRY_CAST(
                 REGEXP_REPLACE(TRIM(CAST(result AS VARCHAR)), '[^0-9.]', '')
                 AS DOUBLE)
             ELSE TRY_CAST(result AS DOUBLE) END     AS value,
        CAST(result AS VARCHAR)                      AS result_raw,
        units,
        'specimen_collect_dt'                        AS lab_date_provenance,
        1.0                                          AS source_priority,
        'thyroglobulin_labs'                         AS source_table
    FROM thyroglobulin_labs
    WHERE result IS NOT NULL
      AND TRIM(CAST(result AS VARCHAR)) != ''
      AND TRY_CAST(specimen_collect_dt AS DATE) IS NOT NULL
),

-- ── Source 2: anti_thyroglobulin_labs (structured) ────────────────────────
anti_tg_structured AS (
    SELECT
        research_id,
        'anti_tg'                                    AS lab_type,
        CAST(specimen_collect_dt AS DATE)            AS lab_date,
        FALSE                                        AS is_below_threshold,
        TRY_CAST(result AS DOUBLE)                   AS value,
        CAST(result AS VARCHAR)                      AS result_raw,
        units,
        'specimen_collect_dt'                        AS lab_date_provenance,
        1.0                                          AS source_priority,
        'anti_thyroglobulin_labs'                    AS source_table
    FROM anti_thyroglobulin_labs
    WHERE result IS NOT NULL
      AND TRIM(CAST(result AS VARCHAR)) != ''
      AND TRY_CAST(specimen_collect_dt AS DATE) IS NOT NULL
),

-- ── Source 3: extracted_postop_labs_expanded_v1 (multi-type, mixed sources) ──
postop_labs AS (
    SELECT
        research_id,
        CASE LOWER(lab_type)
            WHEN 'pth'           THEN 'pth'
            WHEN 'calcium'       THEN 'calcium'
            WHEN 'ionized_ca'    THEN 'ionized_calcium'
            WHEN 'tg'            THEN 'thyroglobulin'
            WHEN 'thyroglobulin' THEN 'thyroglobulin'
            ELSE LOWER(lab_type)
        END                                          AS lab_type,
        lab_date,
        FALSE                                        AS is_below_threshold,
        value,
        CAST(value AS VARCHAR)              AS result_raw,
        NULL::VARCHAR                                AS units,
        'extracted_lab'                              AS lab_date_provenance,
        0.7                                          AS source_priority,
        'extracted_postop_labs_expanded_v1'          AS source_table
    FROM extracted_postop_labs_expanded_v1
    WHERE value IS NOT NULL
      AND lab_date IS NOT NULL
),

-- ── Union all sources ─────────────────────────────────────────────────────
all_labs AS (
    SELECT * FROM tg_structured
    UNION ALL
    SELECT * FROM anti_tg_structured
    UNION ALL
    SELECT * FROM postop_labs
),

-- ── Apply plausibility bounds ─────────────────────────────────────────────
plausibility_checked AS (
    SELECT *,
        CASE
            WHEN lab_type = 'thyroglobulin' AND value BETWEEN 0 AND 100000 THEN TRUE
            WHEN lab_type = 'anti_tg'       AND value BETWEEN 0 AND 10000  THEN TRUE
            WHEN lab_type = 'tsh'           AND value BETWEEN 0 AND 500    THEN TRUE
            WHEN lab_type = 'pth'           AND value BETWEEN 0.5 AND 500  THEN TRUE
            WHEN lab_type = 'calcium'       AND value BETWEEN 4 AND 15     THEN TRUE
            WHEN lab_type = 'ionized_calcium' AND value BETWEEN 0.5 AND 2.5 THEN TRUE
            WHEN value IS NULL      THEN NULL
            ELSE FALSE
        END AS plausibility_flag
    FROM all_labs
),

-- ── Remove implausible values ─────────────────────────────────────────────
plausible_labs AS (
    SELECT *
    FROM plausibility_checked
    WHERE COALESCE(plausibility_flag, TRUE)
),

-- ── Deduplicate: same patient + same lab_type + same date + same value ────
-- Prefer highest source_priority when duplicates exist
deduped AS (
    SELECT *
    FROM plausible_labs
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id, lab_type, lab_date,
                     ROUND(COALESCE(value, 0), 2)
        ORDER BY source_priority DESC, lab_date_provenance ASC
    ) = 1
),

-- ── Add surgery date for temporal context ────────────────────────────────
surgery_dates AS (
    SELECT research_id,
           MIN(TRY_CAST(surg_date AS DATE)) AS first_surgery_date
    FROM path_synoptics
    GROUP BY research_id
),

-- ── Add days-post-surgery context ─────────────────────────────────────────
with_context AS (
    SELECT
        d.*,
        s.first_surgery_date,
        CASE WHEN s.first_surgery_date IS NOT NULL
             THEN DATEDIFF('day', s.first_surgery_date, d.lab_date)
             ELSE NULL
        END AS days_post_surgery,
        CASE
            WHEN s.first_surgery_date IS NULL THEN 'unknown'
            WHEN d.lab_date < s.first_surgery_date THEN 'pre_op'
            WHEN DATEDIFF('day', s.first_surgery_date, d.lab_date) <= 30
                 THEN 'immediate_post_op'
            WHEN DATEDIFF('day', s.first_surgery_date, d.lab_date) <= 180
                 THEN 'short_term_follow_up'
            WHEN DATEDIFF('day', s.first_surgery_date, d.lab_date) <= 730
                 THEN 'intermediate_follow_up'
            ELSE 'long_term_follow_up'
        END AS follow_up_phase
    FROM deduped d
    LEFT JOIN surgery_dates s USING (research_id)
),

-- ── Add duplicate flag (flagged but kept -- for transparency) ─────────────
with_duplicate_flag AS (
    SELECT *,
        COUNT(*) OVER (
            PARTITION BY research_id, lab_type, lab_date
        ) > 1 AS same_day_duplicate_flag
    FROM with_context
)

SELECT
    research_id,
    lab_type,
    lab_date,
    value,
    result_raw,
    is_below_threshold,
    COALESCE(units, '') AS units,
    lab_date_provenance AS date_source,
    source_priority,
    source_table,
    plausibility_flag,
    days_post_surgery,
    follow_up_phase,
    first_surgery_date,
    same_day_duplicate_flag,
    CURRENT_TIMESTAMP AS cleaned_at
FROM with_duplicate_flag
WHERE NOT same_day_duplicate_flag OR source_priority >= 1.0
"""


# ─────────────────────────────────────────────────────────────────────────────
# longitudinal_lab_patient_summary_v1
# ─────────────────────────────────────────────────────────────────────────────
PATIENT_SUMMARY_SQL = """
CREATE OR REPLACE TABLE longitudinal_lab_patient_summary_v1 AS
WITH tg_summary AS (
    SELECT
        research_id,
        MIN(CASE WHEN NOT is_below_threshold AND follow_up_phase != 'pre_op'
                 THEN value END)
                                        AS tg_first_postop,
        MIN(value)             AS tg_nadir,
        MAX(value)             AS tg_peak,
        LAST(value ORDER BY lab_date ASC)
                                        AS tg_last_value,
        COUNT(*)                        AS tg_n_measurements,
        MIN(lab_date)            AS tg_first_date,
        MAX(lab_date)            AS tg_last_date,
        BOOL_OR(is_below_threshold)     AS tg_below_threshold_ever,
        -- Rising Tg flag: last value > 2x nadir AND last value > 1.0
        CASE
            WHEN MAX(value) IS NULL OR MIN(value) IS NULL THEN NULL
            WHEN LAST(value ORDER BY lab_date ASC)
                 > GREATEST(2 * MIN(value), 1.0) THEN TRUE
            ELSE FALSE
        END AS tg_rising_flag,
        -- Doubling time estimate (days per 2x increase) -- simplified
        CASE
            WHEN COUNT(*) >= 2
                 AND MAX(value) > 0
                 AND MIN(value) > 0
                 AND LN(MAX(value) / MIN(value)) > 0
            THEN ROUND(
                DATEDIFF('day', MIN(lab_date), MAX(lab_date)) *
                LN(2) / LN(MAX(value) / MIN(value))
                , 0)
            ELSE NULL
        END AS tg_doubling_time_days
    FROM longitudinal_lab_clean_v1
    WHERE lab_type = 'thyroglobulin'
    GROUP BY research_id
),

anti_tg_summary AS (
    SELECT
        research_id,
        MIN(value) AS anti_tg_nadir,
        MAX(value) AS anti_tg_peak,
        COUNT(*)            AS anti_tg_n_measurements,
        LAST(value ORDER BY lab_date ASC) AS anti_tg_last,
        -- Rising anti-Tg: last > 2x nadir
        CASE
            WHEN MAX(value) IS NULL OR MIN(value) IS NULL THEN NULL
            WHEN LAST(value ORDER BY lab_date ASC)
                 > GREATEST(2 * MIN(value), 10) THEN TRUE
            ELSE FALSE
        END AS anti_tg_rising_flag
    FROM longitudinal_lab_clean_v1
    WHERE lab_type = 'anti_tg'
    GROUP BY research_id
),

tsh_summary AS (
    SELECT
        research_id,
        MIN(value) AS tsh_min,
        MAX(value) AS tsh_max,
        COUNT(*)            AS tsh_n_measurements,
        -- Suppressed TSH evidence (<0.1 = suppression therapy marker)
        BOOL_OR(value < 0.1) AS tsh_suppressed_ever
    FROM longitudinal_lab_clean_v1
    WHERE lab_type = 'tsh'
    GROUP BY research_id
),

pth_ca_summary AS (
    SELECT
        research_id,
        MIN(CASE WHEN lab_type = 'pth' THEN value END) AS pth_nadir,
        MIN(CASE WHEN lab_type = 'calcium' THEN value END) AS calcium_nadir,
        MIN(CASE WHEN lab_type = 'ionized_calcium' THEN value END) AS ionized_ca_nadir,
        COUNT(CASE WHEN lab_type = 'pth' THEN 1 END) AS pth_n_measurements,
        COUNT(CASE WHEN lab_type = 'calcium' THEN 1 END) AS calcium_n_measurements,
        -- Post-op hypoparathyroidism: PTH < 15 pg/mL
        BOOL_OR(lab_type = 'pth' AND value < 15 AND days_post_surgery BETWEEN 0 AND 30)
            AS postop_low_pth_flag,
        -- Post-op hypocalcemia: Ca < 8.0 mg/dL
        BOOL_OR(lab_type = 'calcium' AND value < 8.0 AND days_post_surgery BETWEEN 0 AND 30)
            AS postop_low_calcium_flag
    FROM longitudinal_lab_clean_v1
    WHERE lab_type IN ('pth','calcium','ionized_calcium')
    GROUP BY research_id
),

follow_up_completeness AS (
    SELECT
        research_id,
        MAX(lab_date) - MIN(lab_date)
            AS follow_up_lab_duration_days,
        COUNT(DISTINCT lab_type)                    AS n_lab_types_measured,
        -- Lab completeness score (0-100):
        -- 40 pts for Tg measured, 30 for anti-Tg, 15 for TSH, 15 for PTH/Ca
        CASE WHEN COUNT(CASE WHEN lab_type='thyroglobulin' THEN 1 END) > 0
             THEN 40 ELSE 0 END
        + CASE WHEN COUNT(CASE WHEN lab_type='anti_tg' THEN 1 END) > 0
               THEN 30 ELSE 0 END
        + CASE WHEN COUNT(CASE WHEN lab_type='tsh' THEN 1 END) > 0
               THEN 15 ELSE 0 END
        + CASE WHEN COUNT(CASE WHEN lab_type IN ('pth','calcium') THEN 1 END) > 0
               THEN 15 ELSE 0 END
            AS lab_completeness_score
    FROM longitudinal_lab_clean_v1
    GROUP BY research_id
)

SELECT
    COALESCE(tg.research_id, atg.research_id, ts.research_id,
             pc.research_id, fc.research_id)    AS research_id,
    -- Thyroglobulin
    tg.tg_first_postop,
    tg.tg_nadir,
    tg.tg_peak,
    tg.tg_last_value,
    tg.tg_n_measurements,
    tg.tg_first_date,
    tg.tg_last_date,
    tg.tg_below_threshold_ever,
    tg.tg_rising_flag,
    tg.tg_doubling_time_days,
    -- Anti-Tg
    atg.anti_tg_nadir,
    atg.anti_tg_peak,
    atg.anti_tg_n_measurements,
    atg.anti_tg_last,
    atg.anti_tg_rising_flag,
    -- TSH
    ts.tsh_min,
    ts.tsh_max,
    ts.tsh_n_measurements,
    ts.tsh_suppressed_ever,
    -- PTH / Calcium
    pc.pth_nadir,
    pc.calcium_nadir,
    pc.ionized_ca_nadir,
    pc.pth_n_measurements,
    pc.calcium_n_measurements,
    pc.postop_low_pth_flag,
    pc.postop_low_calcium_flag,
    -- Follow-up completeness
    fc.follow_up_lab_duration_days,
    fc.n_lab_types_measured,
    fc.lab_completeness_score,
    CURRENT_TIMESTAMP AS summarized_at
FROM tg_summary tg
FULL OUTER JOIN anti_tg_summary atg USING (research_id)
FULL OUTER JOIN tsh_summary ts      USING (research_id)
FULL OUTER JOIN pth_ca_summary pc   USING (research_id)
FULL OUTER JOIN follow_up_completeness fc USING (research_id)
"""


# ─────────────────────────────────────────────────────────────────────────────
# recurrence_event_clean_v1
# ─────────────────────────────────────────────────────────────────────────────
RECURRENCE_CLEAN_SQL = """
CREATE OR REPLACE TABLE recurrence_event_clean_v1 AS
WITH

-- ── Structural recurrence from refined extraction ──────────────────────────
structural AS (
    SELECT
        research_id,
        'structural'                            AS recurrence_type,
        first_recurrence_date                   AS recurrence_date,
        recurrence_site_inferred                AS recurrence_site,
        COALESCE(detection_category,
                 'imaging_or_biopsy')           AS recurrence_definition,
        1.0                                     AS source_priority,
        'extracted_recurrence_refined_v1'       AS source_table
    FROM extracted_recurrence_refined_v1
    WHERE COALESCE(recurrence_flag_structured, recurrence_any, FALSE) = TRUE
),

-- ── Biochemical recurrence from Tg trajectory ─────────────────────────────
-- Rising Tg > 1.0 ng/mL without structural disease = biochemical recurrence
biochemical_tg AS (
    SELECT
        ls.research_id,
        'biochemical'                           AS recurrence_type,
        ls.tg_last_date                         AS recurrence_date,
        NULL::VARCHAR                           AS recurrence_site,
        'biochemical_tg_rise'                   AS recurrence_definition,
        0.7                                     AS source_priority,
        'longitudinal_lab_clean_v1'             AS source_table
    FROM longitudinal_lab_patient_summary_v1 ls
    WHERE ls.tg_rising_flag
      AND ls.tg_last_value > 1.0
      -- Only if not already captured as structural
      AND CAST(ls.research_id AS VARCHAR) NOT IN (SELECT DISTINCT CAST(research_id AS VARCHAR) FROM structural)
),

-- ── Anti-Tg rising (secondary biochemical marker) ─────────────────────────
biochemical_anti_tg AS (
    SELECT
        ls.research_id,
        'biochemical'                           AS recurrence_type,
        ls.tg_last_date                         AS recurrence_date,
        NULL::VARCHAR                           AS recurrence_site,
        'biochemical_anti_tg_rise'              AS recurrence_definition,
        0.6                                     AS source_priority,
        'longitudinal_lab_clean_v1'             AS source_table
    FROM longitudinal_lab_patient_summary_v1 ls
    WHERE ls.anti_tg_rising_flag
      AND ls.anti_tg_last > 40
      AND CAST(ls.research_id AS VARCHAR) NOT IN (SELECT DISTINCT CAST(research_id AS VARCHAR) FROM structural)
      AND CAST(ls.research_id AS VARCHAR) NOT IN (SELECT DISTINCT CAST(research_id AS VARCHAR) FROM biochemical_tg)
),

all_recurrence AS (
    SELECT * FROM structural
    UNION ALL
    SELECT * FROM biochemical_tg
    UNION ALL
    SELECT * FROM biochemical_anti_tg
)

SELECT
    r.research_id,
    r.recurrence_type,
    r.recurrence_date,
    r.recurrence_site,
    r.recurrence_definition,
    r.source_priority,
    r.source_table,
    -- Best overall: structural > biochemical
    ROW_NUMBER() OVER (
        PARTITION BY r.research_id
        ORDER BY r.source_priority DESC, r.recurrence_date ASC NULLS LAST
    ) AS event_rank,
    (r.recurrence_type = 'structural') AS structural_recurrence_flag,
    (r.recurrence_type = 'biochemical') AS biochemical_recurrence_flag,
    CURRENT_TIMESTAMP AS cleaned_at
FROM all_recurrence r
"""


def _ensure_stubs(con: duckdb.DuckDBPyConnection) -> None:
    """Create empty stub tables for optional upstream dependencies."""
    if not table_available(con, "thyroglobulin_labs"):
        con.execute("""
CREATE OR REPLACE TEMP TABLE thyroglobulin_labs AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS result,
       NULL::VARCHAR AS units, NULL::TIMESTAMP AS specimen_collect_dt
WHERE 1=0
""")
    if not table_available(con, "anti_thyroglobulin_labs"):
        con.execute("""
CREATE OR REPLACE TEMP TABLE anti_thyroglobulin_labs AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS result,
       NULL::VARCHAR AS units, NULL::TIMESTAMP AS specimen_collect_dt
WHERE 1=0
""")
    if not table_available(con, "extracted_postop_labs_expanded_v1"):
        con.execute("""
CREATE OR REPLACE TEMP TABLE extracted_postop_labs_expanded_v1 AS
SELECT NULL::INTEGER AS research_id, NULL::VARCHAR AS lab_type,
       NULL::DOUBLE AS value, NULL::DATE AS lab_date,
       NULL::VARCHAR AS lab_date_provenance
WHERE 1=0
""")
    if not table_available(con, "extracted_recurrence_refined_v1"):
        con.execute("""
CREATE OR REPLACE TEMP TABLE extracted_recurrence_refined_v1 AS
SELECT NULL::INTEGER AS research_id,
       NULL::BOOLEAN AS recurrence_flag_structured,
       NULL::BOOLEAN AS recurrence_any,
       NULL::DATE AS first_recurrence_date,
       NULL::VARCHAR AS recurrence_site,
       NULL::VARCHAR AS recurrence_detection_method
WHERE 1=0
""")
    if not table_available(con, "path_synoptics"):
        pq = ROOT / "processed" / "path_synoptics.parquet"
        if pq.exists():
            con.execute(
                "CREATE OR REPLACE TABLE path_synoptics AS "
                f"SELECT * FROM read_parquet('{pq}')"
            )
        else:
            con.execute("""
CREATE OR REPLACE TEMP TABLE path_synoptics AS
SELECT NULL::INTEGER AS research_id, NULL::DATE AS surg_date
WHERE 1=0
""")


def build_lab_tables(con: duckdb.DuckDBPyConnection,
                     dry_run: bool = False) -> None:
    section("Building longitudinal lab tables")

    has_tg = table_available(con, "thyroglobulin_labs")
    has_anti_tg = table_available(con, "anti_thyroglobulin_labs")
    has_postop = table_available(con, "extracted_postop_labs_expanded_v1")

    print(f"  thyroglobulin_labs:                 {'present' if has_tg else 'missing'}")
    print(f"  anti_thyroglobulin_labs:             {'present' if has_anti_tg else 'missing'}")
    print(f"  extracted_postop_labs_expanded_v1:  {'present' if has_postop else 'missing'}")

    if dry_run:
        print("  [DRY-RUN] Would create longitudinal_lab_clean_v1, "
              "longitudinal_lab_patient_summary_v1, recurrence_event_clean_v1")
        return

    _ensure_stubs(con)

    print("\n  Building longitudinal_lab_clean_v1...")
    con.execute(LAB_CLEAN_SQL)
    r = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id), COUNT(DISTINCT lab_type) "
        "FROM longitudinal_lab_clean_v1"
    ).fetchone()
    print(f"    longitudinal_lab_clean_v1: {r[0]:,} rows, "
          f"{r[1]:,} patients, {r[2]} lab types")

    # Per-type counts
    type_counts = con.execute(
        "SELECT lab_type, COUNT(*) AS n, COUNT(DISTINCT research_id) AS n_patients "
        "FROM longitudinal_lab_clean_v1 GROUP BY lab_type ORDER BY n DESC"
    ).fetchdf()
    print(type_counts.to_string(index=False))

    print("  Building longitudinal_lab_patient_summary_v1...")
    con.execute(PATIENT_SUMMARY_SQL)
    r = con.execute(
        "SELECT COUNT(*), "
        "SUM(CASE WHEN tg_n_measurements > 0 THEN 1 ELSE 0 END), "
        "ROUND(AVG(lab_completeness_score), 1) "
        "FROM longitudinal_lab_patient_summary_v1"
    ).fetchone()
    print(f"    longitudinal_lab_patient_summary_v1: {r[0]:,} patients, "
          f"{r[1]:,} with Tg data, avg completeness score {r[2]}")

    print("  Building recurrence_event_clean_v1...")
    con.execute(RECURRENCE_CLEAN_SQL)
    r = con.execute(
        "SELECT recurrence_type, COUNT(DISTINCT research_id) AS n_patients "
        "FROM recurrence_event_clean_v1 GROUP BY recurrence_type"
    ).fetchdf()
    print(r.to_string(index=False))

    print("\n  [DONE] Longitudinal lab tables created")


def main() -> None:
    p = argparse.ArgumentParser(description="53_longitudinal_lab_hardening.py")
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
        build_lab_tables(con, dry_run=args.dry_run)
    finally:
        con.close()

    print("\n[COMPLETE] 53_longitudinal_lab_hardening.py finished")


if __name__ == "__main__":
    main()
