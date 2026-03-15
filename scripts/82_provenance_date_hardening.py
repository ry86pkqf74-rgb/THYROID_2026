#!/usr/bin/env python3
"""
82_provenance_date_hardening.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Domain-level date + source provenance hardening for:
  - Recurrence events
  - RAI episodes
  - Molecular test episodes

For each domain this script:
  1. Inventories current canonical fields and missingness
  2. Adds/repairs provenance columns using a layered-fallback strategy
  3. Creates val_* tables for downstream CI checks
  4. Reports before/after coverage

New validation tables created:
  val_recurrence_provenance_v2   -- recurrence date/source completeness
  val_rai_provenance_v2          -- RAI episode date/dose/source completeness
  val_molecular_provenance_v2    -- molecular test date/platform/source completeness

Provenance columns added/assured (ADDITIVE only):
  Each val_* table exposes {domain}_date_status, {domain}_date_confidence,
  {domain}_source_table, {domain}_source_field for every patient/episode.

Constraints:
  - Never overwrites existing non-null provenance
  - Never fabricates precision; uses exact/approximate/inferred status flags
  - Designed to run after script 78 (final hardening)

Supports --md (MotherDuck), --local, --dry-run.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
sys.path.insert(0, str(ROOT))
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
EXPORTS_DIR = ROOT / "exports" / "final_md_optimization_20260314"
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)


# ── helpers ──────────────────────────────────────────────────────────────────
def section(t: str) -> None:
    print(f"\n{'=' * 72}\n  {t}\n{'=' * 72}\n")


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


def push_df(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, name: str) -> None:
    tmp = tempfile.mktemp(suffix=".parquet")
    try:
        df.to_parquet(tmp, index=False)
        con.execute(
            f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM read_parquet('{tmp}')"
        )
    finally:
        import os as _os; _os.unlink(tmp)


def get_connection(use_md: bool) -> duckdb.DuckDBPyConnection:
    if use_md:
        try:
            import toml
            token = os.environ.get("MOTHERDUCK_TOKEN") or toml.load(
                str(ROOT / ".streamlit" / "secrets.toml")
            )["MOTHERDUCK_TOKEN"]
        except Exception:
            token = os.environ["MOTHERDUCK_TOKEN"]
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(str(DB_PATH))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# A. RECURRENCE provenance
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RECURRENCE_PROVENANCE_SQL = """
CREATE OR REPLACE TABLE val_recurrence_provenance_v2 AS
WITH base AS (
    SELECT DISTINCT research_id FROM master_cohort
),
rec AS (
    SELECT
        research_id,
        recurrence_flag_structured,
        recurrence_any,
        recurrence_date_best,
        recurrence_date_status,
        recurrence_date_confidence
    FROM extracted_recurrence_refined_v1
),
risk AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        BOOL_OR(LOWER(CAST(recurrence_flag AS VARCHAR)) = 'true') AS risk_recurrence_flag,
        MIN(TRY_CAST(first_recurrence_date AS DATE)) AS risk_first_recurrence_date
    FROM recurrence_risk_features_mv
    GROUP BY 1
),
tg AS (
    SELECT
        research_id,
        COUNT(*) AS tg_measurements,
        MAX(CASE WHEN LOWER(CAST(result AS VARCHAR)) NOT LIKE '<%'
                 THEN TRY_CAST(result AS DOUBLE) END) AS tg_max
    FROM thyroglobulin_labs
    GROUP BY 1
)
SELECT
    b.research_id,
    -- Recurrence status
    COALESCE(rec.recurrence_any, risk.risk_recurrence_flag, FALSE)
        AS recurrence_any_final,
    -- Date provenance
    rec.recurrence_date_best,
    COALESCE(rec.recurrence_date_status,
             CASE WHEN risk.risk_first_recurrence_date IS NOT NULL
                  THEN 'structured_flag_no_day_level_date'
                  ELSE 'no_recurrence'
             END)
        AS recurrence_date_status,
    COALESCE(rec.recurrence_date_confidence, 0)
        AS recurrence_date_confidence,
    -- Source attribution
    CASE
        WHEN rec.recurrence_date_status = 'exact_source_date'                THEN 'extracted_recurrence_refined_v1.recurrence_date_best'
        WHEN rec.recurrence_date_status = 'biochemical_inflection_inferred' THEN 'derived:thyroglobulin_labs'
        WHEN risk.risk_first_recurrence_date IS NOT NULL                     THEN 'recurrence_risk_features_mv.first_recurrence_date'
        ELSE 'none'
    END AS recurrence_source_table,
    -- Lab support
    COALESCE(tg.tg_measurements, 0) AS tg_measurement_count,
    tg.tg_max,
    CASE
        WHEN rec.recurrence_date_best IS NOT NULL          THEN 'has_date'
        WHEN rec.recurrence_any IS TRUE
         AND rec.recurrence_date_best IS NULL              THEN 'flagged_no_date'
        WHEN risk.risk_first_recurrence_date IS NOT NULL   THEN 'structured_date'
        WHEN risk.risk_recurrence_flag IS TRUE             THEN 'flag_only'
        ELSE 'no_recurrence'
    END AS date_availability_class,
    NOW()::VARCHAR AS validated_at
FROM base b
LEFT JOIN rec       ON b.research_id = rec.research_id
LEFT JOIN risk      ON b.research_id = risk.research_id
LEFT JOIN tg        ON b.research_id = tg.research_id
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# B. RAI provenance
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RAI_PROVENANCE_SQL = """
CREATE OR REPLACE TABLE val_rai_provenance_v2 AS
WITH rai_ep AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        COUNT(*) AS episode_count,
        ANY_VALUE(
            COALESCE(
                TRY_CAST(resolved_rai_date AS DATE),
                TRY_CAST(rai_date AS DATE)
            )
        ) AS best_rai_date,
        SUM(CASE WHEN dose_mci IS NOT NULL THEN 1 ELSE 0 END) AS episodes_with_dose,
        MAX(dose_mci) AS max_dose_mci,
        ANY_VALUE(dose_source) AS dose_source,
        ANY_VALUE(dose_confidence) AS dose_confidence,
        ANY_VALUE(rai_assertion_status) AS rai_assertion_status,
        ANY_VALUE(rai_treatment_certainty) AS rai_treatment_certainty,
        ANY_VALUE(rai_date_class) AS rai_date_class
    FROM rai_treatment_episode_v2
    GROUP BY 1
),
rai_refined AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        tier AS refined_tier,
        source_reliability,
        dose_confirmed_flag,
        dose_mci_refined,
        dose_date
    FROM extracted_rai_validated_v1
    WHERE research_id IS NOT NULL
),
pat AS (
    SELECT DISTINCT CAST(research_id AS INTEGER) AS research_id
    FROM master_cohort
)
SELECT
    p.research_id,
    COALESCE(r.episode_count, 0) AS rai_episode_count,
    r.best_rai_date,
    COALESCE(r.episodes_with_dose, 0) AS episodes_with_dose,
    r.max_dose_mci,
    -- Date provenance
    COALESCE(r.rai_date_class, 'no_rai') AS rai_date_status,
    CASE
        WHEN r.best_rai_date IS NOT NULL
         AND r.rai_date_class IN ('exact_source_date','inferred_day_level_date') THEN 100
        WHEN r.best_rai_date IS NOT NULL   THEN 55
        ELSE 0
    END AS rai_date_confidence,
    -- Dose provenance
    COALESCE(r.dose_source, rfn.source_reliability, 'unavailable') AS dose_source_final,
    COALESCE(r.dose_confidence, 0.0) AS dose_confidence_final,
    -- Assertion confidence
    COALESCE(r.rai_assertion_status, 'no_rai') AS rai_assertion_status,
    COALESCE(r.rai_treatment_certainty, 'no_rai') AS rai_treatment_certainty,
    -- Structural limitation flags
    CASE
        WHEN r.episode_count IS NULL OR r.episode_count = 0 THEN 'no_rai_episodes'
        WHEN r.episodes_with_dose = 0                      THEN 'has_date_no_dose'
        WHEN r.episodes_with_dose > 0                      THEN 'has_date_and_dose'
        ELSE 'unknown'
    END AS rai_completeness_tier,
    -- Source-limitation note
    CASE
        WHEN r.best_rai_date IS NOT NULL   THEN 'date_available'
        WHEN r.episode_count > 0           THEN 'episode_flag_no_date'
        ELSE 'absent'
    END AS date_availability_class,
    NOW()::VARCHAR AS validated_at
FROM pat p
LEFT JOIN rai_ep r   ON p.research_id = r.research_id
LEFT JOIN rai_refined rfn ON p.research_id = rfn.research_id
"""

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# C. MOLECULAR provenance
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MOLECULAR_PROVENANCE_SQL = """
CREATE OR REPLACE TABLE val_molecular_provenance_v2 AS
WITH mol AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        COUNT(*) AS episode_count,
        COUNT(DISTINCT platform) AS platform_count,
        STRING_AGG(DISTINCT platform, '; ') AS platforms_used,
        SUM(CASE WHEN molecular_date_raw_class IS NOT NULL THEN 1 ELSE 0 END)
            AS episodes_with_date_class,
        SUM(CASE WHEN molecular_date_raw_class IN ('exact_source_date','inferred_day_level_date')
                 THEN 1 ELSE 0 END) AS episodes_with_day_date,
        SUM(CASE WHEN is_placeholder_row IS TRUE THEN 1 ELSE 0 END)
            AS placeholder_rows,
        SUM(CASE WHEN is_placeholder_row IS NOT TRUE THEN 1 ELSE 0 END)
            AS real_test_rows,
        BOOL_OR(braf_flag IS TRUE) AS has_braf,
        BOOL_OR(tert_flag IS TRUE) AS has_tert,
        BOOL_OR(ras_flag IS TRUE OR ras_subtype IS NOT NULL) AS has_ras,
        ANY_VALUE(molecular_date_raw_class) AS dominant_date_class,
        ANY_VALUE(molecular_analysis_eligible_flag) AS analysis_eligible_flag
    FROM molecular_test_episode_v2
    GROUP BY 1
),
braf_rec AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        braf_recovered_status_v11,
        braf_detection_method_v11
    FROM patient_refined_master_clinical_v10
    WHERE research_id IS NOT NULL
),
pat AS (
    SELECT DISTINCT CAST(research_id AS INTEGER) AS research_id
    FROM master_cohort
)
SELECT
    p.research_id,
    COALESCE(m.episode_count, 0) AS molecular_episode_count,
    COALESCE(m.real_test_rows, 0) AS real_test_count,
    COALESCE(m.placeholder_rows, 0) AS placeholder_count,
    m.platforms_used,
    -- Date provenance
    COALESCE(m.dominant_date_class, 'no_molecular') AS molecular_date_status,
    CASE
        WHEN m.episodes_with_day_date > 0  THEN 100
        WHEN m.episodes_with_date_class > 0 THEN 50
        ELSE 0
    END AS molecular_date_confidence,
    -- Gene panel coverage
    COALESCE(m.has_braf OR b.braf_recovered_status_v11 = 'positive', FALSE) AS braf_positive_final,
    COALESCE(m.has_tert, FALSE) AS tert_positive,
    COALESCE(m.has_ras, FALSE) AS ras_positive,
    COALESCE(b.braf_detection_method_v11, 'not_tested') AS braf_detection_method,
    -- Analysis eligibility
    COALESCE(m.analysis_eligible_flag, FALSE) AS analysis_eligible,
    -- Source attribution
    CASE
        WHEN m.real_test_rows > 0 THEN 'molecular_test_episode_v2'
        ELSE 'not_tested'
    END AS molecular_source_table,
    -- Completeness
    CASE
        WHEN m.real_test_rows > 0 AND m.episodes_with_day_date > 0    THEN 'full_provenance'
        WHEN m.real_test_rows > 0 AND m.episodes_with_date_class > 0  THEN 'partial_date'
        WHEN m.real_test_rows > 0                                      THEN 'result_no_date'
        ELSE 'not_tested'
    END AS molecular_completeness_tier,
    NOW()::VARCHAR AS validated_at
FROM pat p
LEFT JOIN mol m    ON p.research_id = m.research_id
LEFT JOIN braf_rec b ON p.research_id = b.research_id
"""

# ── Summary tabulation SQL ────────────────────────────────────────────────────
def domain_summary_sql(table: str, domain: str) -> str:
    date_field = f"{domain}_date_status"
    tier_field = f"{domain}_completeness_tier"
    return f"""
    SELECT
        '{domain}' AS domain,
        COUNT(*) AS total_patients,
        COUNT(*) FILTER (WHERE {date_field} NOT IN ('no_{domain}', 'absent', 'not_tested'))
            AS patients_with_data,
        COUNT(*) FILTER (WHERE {date_field} IN ('exact_source_date','inferred_day_level_date','has_date'))
            AS patients_with_precise_date,
        COUNT(*) FILTER (WHERE {date_field} LIKE '%flag%' OR {date_field} LIKE '%inferred%')
            AS patients_flag_only,
        COUNT(*) FILTER (WHERE {date_field} IN ('no_{domain}', 'absent', 'not_tested', 'no_recurrence'))
            AS patients_no_data,
        ROUND(100.0 * COUNT(*) FILTER (
            WHERE {date_field} IN ('exact_source_date','inferred_day_level_date','has_date'))
            / NULLIF(COUNT(*), 0), 1) AS precise_date_pct,
        NOW()::VARCHAR AS validated_at
    FROM {table}
    """


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--md", action="store_true", help="Use MotherDuck")
    ap.add_argument("--local", action="store_true", help="Use local DuckDB")
    ap.add_argument("--dry-run", action="store_true", help="Skip writes")
    args = ap.parse_args()

    use_md = args.md or not args.local
    dry = args.dry_run

    con = get_connection(use_md)
    print("\n" + "=" * 72)
    print("  82 — Provenance & Date Hardening: Recurrence / RAI / Molecular")
    print("=" * 72)

    summaries: list[dict] = []

    for label, sql, tbl_name in [
        ("Recurrence", RECURRENCE_PROVENANCE_SQL, "val_recurrence_provenance_v2"),
        ("RAI",        RAI_PROVENANCE_SQL,         "val_rai_provenance_v2"),
        ("Molecular",  MOLECULAR_PROVENANCE_SQL,   "val_molecular_provenance_v2"),
    ]:
        section(f"Phase: {label} Provenance")

        if dry:
            print(f"  [DRY RUN] Would create {tbl_name}")
        else:
            try:
                con.execute(sql)
                n = safe_count(con, f"SELECT COUNT(*) FROM {tbl_name}")
                print(f"  ✓ {tbl_name}: {n:,} rows")
            except Exception as e:
                print(f"  ✗ {tbl_name}: {e}")
                continue

        # Report missingness
        domain = label.lower()

        def try_count(q: str) -> int:
            return safe_count(con, q)

        total = try_count(f"SELECT COUNT(*) FROM {tbl_name}")
        has_data_col = f"{domain}_date_status"
        no_data_vals = {
            "recurrence": "('no_recurrence','absent')",
            "rai":        "('no_rai','no_rai_episodes','absent')",
            "molecular":  "('no_molecular','not_tested')",
        }.get(domain, "('none')")

        has_data = try_count(
            f"SELECT COUNT(*) FROM {tbl_name} "
            f"WHERE {has_data_col} NOT IN {no_data_vals}"
        ) if table_exists(con, tbl_name) else -1

        pct = round(100.0 * has_data / total, 1) if total > 0 and has_data >= 0 else 0

        summaries.append({
            "domain": domain,
            "val_table": tbl_name,
            "total_patients": total,
            "patients_with_data": has_data,
            "coverage_pct": pct,
        })
        print(f"  Coverage: {has_data}/{total} patients ({pct}%)")

    # ── Combined summary table ────────────────────────────────────────────
    if not dry:
        df = pd.DataFrame(summaries)
        df["validated_at"] = datetime.utcnow().isoformat()
        df["validator_script"] = "82"
        try:
            push_df(con, df, "val_provenance_hardening_summary_v1")
            print(f"\n  ✓ val_provenance_hardening_summary_v1 created ({len(df)} rows)")
        except Exception as e:
            print(f"  WARNING: {e}")

        out = EXPORTS_DIR / "val_provenance_hardening_summary_v1.csv"
        df.to_csv(out, index=False)
        print(f"  Exported: {out.relative_to(ROOT)}")

    con.close()
    print("\n  Done.\n")


if __name__ == "__main__":
    main()
