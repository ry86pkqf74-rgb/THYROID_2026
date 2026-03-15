#!/usr/bin/env python3
"""
98_final_verification_pass.py — Comprehensive final verification pass

Combines:
  A) Canonical truth snapshot
  B) Multi-surgery episode linkage integrity audit
  C) Operative NLP propagation audit
  D) Recurrence review packet export
  E) Documentation reconciliation

Connects to live MotherDuck prod, generates all deliverables, and identifies
discrepancies between documentation and live data.

Usage:
    .venv/bin/python scripts/98_final_verification_pass.py --md
"""
from __future__ import annotations

import argparse
import csv
import datetime
import json
import os
import pathlib
import re
import sys
import textwrap

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    import toml
except ImportError:
    toml = None

try:
    import duckdb
except ImportError:
    sys.exit("duckdb not installed — run from .venv/bin/python")

NOW = datetime.datetime.now()
DATESTAMP = NOW.strftime("%Y%m%d")
TIMESTAMP = NOW.strftime("%Y%m%d_%H%M")

EXPORT_TRUTH_DIR     = pathlib.Path(f"exports/dataset_truth_snapshot_{DATESTAMP}")
EXPORT_LINKAGE_DIR   = pathlib.Path(f"exports/multi_surgery_linkage_audit_{TIMESTAMP}")
EXPORT_RECURRENCE    = pathlib.Path("exports/recurrence_review_packets")
DOCS                 = pathlib.Path("docs")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def get_md_token() -> str:
    for key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN"):
        tok = os.environ.get(key, "")
        if tok:
            return tok
    if toml:
        for p in (ROOT / ".streamlit" / "secrets.toml",
                   pathlib.Path.home() / ".streamlit" / "secrets.toml"):
            if p.exists():
                try:
                    return toml.load(str(p)).get("MOTHERDUCK_TOKEN", "")
                except Exception:
                    pass
    return ""


def connect_md(db: str = "thyroid_research_2026") -> duckdb.DuckDBPyConnection:
    tok = get_md_token()
    if not tok:
        sys.exit("MOTHERDUCK_TOKEN not found")
    os.environ["MOTHERDUCK_TOKEN"] = tok
    con = duckdb.connect(f"md:{db}?motherduck_token={tok}")
    print(f"✓ Connected to md:{db}")
    return con


def q1(con, sql):
    """Single scalar."""
    try:
        r = con.execute(sql).fetchone()
        return r[0] if r else None
    except Exception as e:
        return f"ERR:{e}"


def qall(con, sql):
    try:
        return con.execute(sql).fetchall()
    except Exception as e:
        return f"ERR:{e}"


def qdf(con, sql):
    try:
        desc = con.execute(sql).description
        cols = [d[0] for d in desc]
        rows = con.execute(sql).fetchall()
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        return f"ERR:{e}"


def si(v):
    """Safe int."""
    if v is None or isinstance(v, str):
        return 0
    return int(v)


def sp(num, den, d=1):
    """Safe percent."""
    return round(100.0 * num / den, d) if den > 0 else 0.0


def tbl_exists(con, tbl):
    try:
        con.execute(f"SELECT 1 FROM {tbl} LIMIT 0")
        return True
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# WORKSTREAM A — CANONICAL TRUTH SNAPSHOT
# ═══════════════════════════════════════════════════════════════════════════════

METRIC_SQL = {
    "total_patients":            "SELECT COUNT(DISTINCT research_id) FROM patient_analysis_resolved_v1",
    "surgical_cohort":           "SELECT COUNT(DISTINCT research_id) FROM path_synoptics",
    "analysis_cancer_cohort":    "SELECT COUNT(*) FROM analysis_cancer_cohort_v1",
    "manuscript_cohort":         "SELECT COUNT(*) FROM manuscript_cohort_v1",
    "episode_dedup":             "SELECT COUNT(*) FROM episode_analysis_resolved_v1_dedup",
    "scoring_rows":              "SELECT COUNT(*) FROM thyroid_scoring_py_v1",
    "survival_cohort_enriched":  "SELECT COUNT(*) FROM survival_cohort_enriched",
    # multi-surgery
    "multi_surg_patients":       """SELECT COUNT(*) FROM (
                                    SELECT research_id FROM path_synoptics
                                    GROUP BY research_id HAVING COUNT(DISTINCT surg_date) > 1)""",
    "multi_surg_episodes":       """SELECT COUNT(*) FROM (
                                    SELECT research_id, surg_date FROM path_synoptics
                                    GROUP BY research_id, surg_date
                                    HAVING research_id IN (
                                        SELECT research_id FROM path_synoptics
                                        GROUP BY research_id HAVING COUNT(DISTINCT surg_date) > 1))""",
    # recurrence
    "recurrence_flagged":        "SELECT COUNT(*) FROM extracted_recurrence_refined_v1 WHERE recurrence_any IS TRUE",
    "recurrence_exact":          "SELECT COUNT(*) FROM extracted_recurrence_refined_v1 WHERE recurrence_date_status = 'exact_source_date'",
    "recurrence_biochem":        "SELECT COUNT(*) FROM extracted_recurrence_refined_v1 WHERE recurrence_date_status = 'biochemical_inflection_inferred'",
    "recurrence_unresolved":     "SELECT COUNT(*) FROM extracted_recurrence_refined_v1 WHERE recurrence_date_status = 'unresolved_date'",
    # RAI
    "rai_episodes":              "SELECT COUNT(*) FROM rai_treatment_episode_v2",
    "rai_with_dose":             "SELECT COUNT(*) FROM rai_treatment_episode_v2 WHERE dose_mci IS NOT NULL AND dose_mci > 0",
    # molecular
    "molecular_tested":          "SELECT COUNT(DISTINCT research_id) FROM extracted_molecular_panel_v1",
    "braf_positive":             "SELECT COUNT(*) FROM patient_refined_master_clinical_v12 WHERE braf_positive_final IS TRUE",
    "ras_positive":              "SELECT COUNT(*) FROM extracted_ras_patient_summary_v1 WHERE ras_positive IS TRUE",
    "tert_positive":             "SELECT COUNT(*) FROM extracted_molecular_refined_v1 WHERE tert_positive_refined IS TRUE",
    # imaging
    "tirads_patients":           "SELECT COUNT(*) FROM extracted_tirads_validated_v1",
    "imaging_nodule_rows":       "SELECT COUNT(*) FROM imaging_nodule_master_v1",
    "imaging_fna_linkage":       "SELECT COUNT(*) FROM imaging_fna_linkage_v3",
    # labs
    "lab_canonical_rows":        "SELECT COUNT(*) FROM longitudinal_lab_canonical_v1",
    "lab_canonical_patients":    "SELECT COUNT(DISTINCT research_id) FROM longitudinal_lab_canonical_v1",
    "tg_patients":               "SELECT COUNT(DISTINCT research_id) FROM thyroglobulin_labs",
    # adjudication
    "adjudication_decisions":    "SELECT COUNT(*) FROM adjudication_decisions",
    # complications
    "complications_refined":     "SELECT COUNT(*) FROM extracted_complications_refined_v5",
    "complications_patients":    "SELECT COUNT(DISTINCT research_id) FROM patient_refined_complication_flags_v2",
    # master/demo
    "master_v12_rows":           "SELECT COUNT(*) FROM patient_refined_master_clinical_v12",
    "demographics_rows":         "SELECT COUNT(*) FROM demographics_harmonized_v2",
    "md_table_count":            "SELECT COUNT(DISTINCT table_name) FROM information_schema.tables WHERE table_schema = 'main'",
    # operative NLP coverage
    "op_rln_monitoring":         "SELECT SUM(CASE WHEN rln_monitoring_flag IS TRUE THEN 1 ELSE 0 END) FROM operative_episode_detail_v2",
    "op_drain":                  "SELECT SUM(CASE WHEN drain_flag IS TRUE THEN 1 ELSE 0 END) FROM operative_episode_detail_v2",
    "op_findings_raw":           "SELECT COUNT(*) FROM operative_episode_detail_v2 WHERE operative_findings_raw IS NOT NULL AND operative_findings_raw != ''",
    "op_episodes_total":         "SELECT COUNT(*) FROM operative_episode_detail_v2",
}


def workstream_a(con) -> dict:
    print("\n" + "=" * 78)
    print("  WORKSTREAM A — CANONICAL TRUTH SNAPSHOT")
    print("=" * 78)
    metrics = {}
    for name, sql in METRIC_SQL.items():
        val = q1(con, sql)
        metrics[name] = val
        label = name.replace("_", " ").title()
        print(f"  {label}: {val}")
    return metrics


# ═══════════════════════════════════════════════════════════════════════════════
# WORKSTREAM B — MULTI-SURGERY EPISODE INTEGRITY
# ═══════════════════════════════════════════════════════════════════════════════

def workstream_b(con, metrics) -> dict:
    print("\n" + "=" * 78)
    print("  WORKSTREAM B — MULTI-SURGERY EPISODE INTEGRITY")
    print("=" * 78)

    results = {}

    # ── 1. Build multi-surgery cohort ──────────────────────────────────────
    print("\n  B.1 Building multi-surgery cohort...")
    ms_cohort_sql = """
    CREATE OR REPLACE TABLE val_multi_surgery_cohort_v2 AS
    WITH surgeries AS (
        SELECT research_id,
               surg_date,
               ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY TRY_CAST(surg_date AS DATE), surg_date) AS surgery_episode_id
        FROM (SELECT DISTINCT research_id, surg_date FROM path_synoptics WHERE surg_date IS NOT NULL)
    ),
    multi AS (
        SELECT research_id FROM surgeries GROUP BY research_id HAVING MAX(surgery_episode_id) > 1
    )
    SELECT s.research_id,
           s.surgery_episode_id,
           s.surg_date,
           TRY_CAST(s.surg_date AS DATE) AS surg_date_parsed,
           LEAD(TRY_CAST(s.surg_date AS DATE)) OVER (PARTITION BY s.research_id ORDER BY s.surgery_episode_id) AS next_surg_date,
           (LEAD(TRY_CAST(s.surg_date AS DATE)) OVER (PARTITION BY s.research_id ORDER BY s.surgery_episode_id) -
            TRY_CAST(s.surg_date AS DATE))::INT AS inter_surgery_days,
           MAX(s.surgery_episode_id) OVER (PARTITION BY s.research_id) AS n_surgeries
    FROM surgeries s
    WHERE s.research_id IN (SELECT research_id FROM multi)
    ORDER BY s.research_id, s.surgery_episode_id
    """
    try:
        con.execute(ms_cohort_sql)
        ms_count = q1(con, "SELECT COUNT(*) FROM val_multi_surgery_cohort_v2")
        ms_patients = q1(con, "SELECT COUNT(DISTINCT research_id) FROM val_multi_surgery_cohort_v2")
        print(f"    Episodes: {ms_count}, Patients: {ms_patients}")
        results["cohort_episodes"] = si(ms_count)
        results["cohort_patients"] = si(ms_patients)
    except Exception as e:
        print(f"    ERROR building cohort: {e}")
        results["cohort_error"] = str(e)
        return results

    # ── 2. Audit artifact assignment by domain ─────────────────────────────
    print("\n  B.2 Auditing artifact assignment per domain...")

    # Define domains and their artifact tables + date columns
    ARTIFACT_DOMAINS = [
        ("pathology", "path_synoptics", "TRY_CAST(surg_date AS DATE)", "1:1 surgery-pathology"),
        ("operative", "operative_episode_detail_v2", "COALESCE(TRY_CAST(resolved_surgery_date AS DATE), surgery_date_native)", "1:1 surgery-operative"),
        ("fna", "fna_episode_master_v2", "TRY_CAST(resolved_fna_date AS DATE)", "many:1 FNA→surgery"),
        ("molecular", "molecular_test_episode_v2", "TRY_CAST(resolved_test_date AS DATE)", "many:1 molecular→surgery"),
        ("rai", "rai_treatment_episode_v2", "TRY_CAST(resolved_rai_date AS DATE)", "many:1 RAI→surgery"),
        ("imaging", "imaging_nodule_master_v1", "NULL", "imaging-level"),
        ("lab_tg", "thyroglobulin_labs", "TRY_CAST(specimen_collect_dt AS DATE)", "many:1 labs→surgery"),
        ("lab_canonical", "longitudinal_lab_canonical_v1", "TRY_CAST(lab_date AS DATE)", "many:1 labs→surgery"),
    ]

    domain_results = []
    for domain, tbl, date_expr, desc in ARTIFACT_DOMAINS:
        if not tbl_exists(con, tbl):
            print(f"    {domain}: SKIP (table {tbl} missing)")
            domain_results.append({"domain": domain, "total_ms": 0, "status": "TABLE_MISSING"})
            continue

        # Count multi-surgery patients' artifacts
        try:
            sql = f"""
            SELECT COUNT(*) FROM {tbl} a
            WHERE CAST(a.research_id AS VARCHAR) IN (SELECT DISTINCT CAST(research_id AS VARCHAR) FROM val_multi_surgery_cohort_v2)
            """
            total_ms = si(q1(con, sql))
        except Exception as e:
            total_ms = 0
            print(f"    {domain}: ERROR counting: {e}")

        # For date-based domains, compute episode assignment quality
        uniquely_linked = 0
        ambiguous = 0
        unlinked = 0
        mislinked = 0

        if date_expr != "NULL" and total_ms > 0:
            try:
                assign_sql = f"""
                WITH art AS (
                    SELECT a.research_id, {date_expr} AS art_date
                    FROM {tbl} a
                    WHERE CAST(a.research_id AS VARCHAR) IN (SELECT DISTINCT CAST(research_id AS VARCHAR) FROM val_multi_surgery_cohort_v2)
                      AND {date_expr} IS NOT NULL
                ),
                scored AS (
                    SELECT art.research_id, art.art_date,
                           c.surgery_episode_id, c.surg_date_parsed,
                           ABS(DATE_DIFF('day', art.art_date, c.surg_date_parsed)) AS days_gap,
                           ROW_NUMBER() OVER (PARTITION BY art.research_id, art.art_date
                                              ORDER BY ABS(DATE_DIFF('day', art.art_date, c.surg_date_parsed))) AS rk
                    FROM art
                    JOIN val_multi_surgery_cohort_v2 c
                      ON CAST(art.research_id AS VARCHAR) = CAST(c.research_id AS VARCHAR)
                ),
                best AS (
                    SELECT s1.research_id, s1.art_date, s1.surgery_episode_id, s1.days_gap,
                           (SELECT MIN(s2.days_gap) FROM scored s2
                            WHERE s2.research_id = s1.research_id AND s2.art_date = s1.art_date
                              AND s2.surgery_episode_id != s1.surgery_episode_id) AS second_best_gap
                    FROM scored s1
                    WHERE s1.rk = 1
                )
                SELECT
                    SUM(CASE WHEN days_gap <= 30 AND (second_best_gap IS NULL OR second_best_gap - days_gap > 14) THEN 1 ELSE 0 END) AS uniquely_linked,
                    SUM(CASE WHEN second_best_gap IS NOT NULL AND ABS(second_best_gap - days_gap) <= 14 THEN 1 ELSE 0 END) AS ambiguous,
                    SUM(CASE WHEN days_gap > 365 THEN 1 ELSE 0 END) AS unlinked,
                    SUM(CASE WHEN days_gap > 30 AND days_gap <= 365 AND (second_best_gap IS NULL OR second_best_gap - days_gap > 14) THEN 1 ELSE 0 END) AS distant_linked
                FROM best
                """
                row = con.execute(assign_sql).fetchone()
                if row:
                    uniquely_linked = si(row[0])
                    ambiguous = si(row[1])
                    unlinked = si(row[2])
                    mislinked = si(row[3])
            except Exception as e:
                print(f"    {domain}: assignment audit error: {e}")

        domain_results.append({
            "domain": domain,
            "table": tbl,
            "description": desc,
            "total_ms_artifacts": total_ms,
            "uniquely_linked": uniquely_linked,
            "ambiguous": ambiguous,
            "distant_linked": mislinked,
            "unlinked": unlinked,
        })
        print(f"    {domain}: total={total_ms}, unique={uniquely_linked}, ambig={ambiguous}, distant={mislinked}, unlinked={unlinked}")

    results["domains"] = domain_results

    # ── 3. Episode key propagation ─────────────────────────────────────────
    print("\n  B.3 Episode key propagation check...")
    KEY_TABLES = [
        ("operative_episode_detail_v2", "surgery_episode_id"),
        ("episode_analysis_resolved_v1_dedup", "surgery_episode_id"),
        ("tumor_episode_master_v2", "surgery_episode_id"),
    ]
    propagation = []
    for tbl, col in KEY_TABLES:
        if not tbl_exists(con, tbl):
            propagation.append({"table": tbl, "column": col, "status": "MISSING"})
            continue
        try:
            total = si(q1(con, f"SELECT COUNT(*) FROM {tbl} WHERE research_id IN (SELECT DISTINCT research_id FROM val_multi_surgery_cohort_v2)"))
            distinct_ids = si(q1(con, f"SELECT COUNT(DISTINCT {col}) FROM {tbl} WHERE research_id IN (SELECT DISTINCT research_id FROM val_multi_surgery_cohort_v2)"))
            propagation.append({"table": tbl, "column": col, "ms_rows": total, "distinct_episode_ids": distinct_ids,
                                "status": "CORRECT" if distinct_ids > 1 else "ALL_EPISODE_1"})
            print(f"    {tbl}.{col}: {total} rows, {distinct_ids} distinct IDs → {'CORRECT' if distinct_ids > 1 else 'ALL_EPISODE_1'}")
        except Exception as e:
            propagation.append({"table": tbl, "column": col, "status": f"ERROR: {e}"})
    results["propagation"] = propagation

    # ── 4. Create review queue for high-risk cases ─────────────────────────
    print("\n  B.4 Creating high-risk review queue...")
    try:
        con.execute("""
        CREATE OR REPLACE TABLE val_multi_surgery_review_queue_v2 AS
        WITH per_patient AS (
            SELECT c.research_id, c.n_surgeries,
                   COUNT(DISTINCT ps.surg_date) AS ps_distinct_dates,
                   COUNT(*) AS ps_rows
            FROM val_multi_surgery_cohort_v2 c
            LEFT JOIN path_synoptics ps ON c.research_id = ps.research_id
            GROUP BY c.research_id, c.n_surgeries
        )
        SELECT DISTINCT research_id, n_surgeries, ps_distinct_dates, ps_rows,
               CASE
                   WHEN ps_distinct_dates < n_surgeries THEN 'DATE_MISMATCH'
                   WHEN ps_rows > n_surgeries * 2 THEN 'EXCESSIVE_PATHOLOGY'
                   ELSE 'STANDARD'
               END AS review_reason
        FROM per_patient
        WHERE ps_distinct_dates < n_surgeries OR ps_rows > n_surgeries * 2
        ORDER BY n_surgeries DESC, research_id
        """)
        review_q = si(q1(con, "SELECT COUNT(*) FROM val_multi_surgery_review_queue_v2"))
        results["review_queue_rows"] = review_q
        print(f"    Review queue: {review_q} patients")
    except Exception as e:
        print(f"    Review queue error: {e}")
        results["review_queue_error"] = str(e)

    # ── 5. Run ANALYZE on new tables ───────────────────────────────────────
    for tbl in ["val_multi_surgery_cohort_v2", "val_multi_surgery_review_queue_v2"]:
        try:
            con.execute(f"ANALYZE {tbl}")
        except Exception:
            pass

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# WORKSTREAM C — OPERATIVE NLP PROPAGATION
# ═══════════════════════════════════════════════════════════════════════════════

OPERATIVE_FIELDS = [
    ("rln_monitoring_flag", True),
    ("rln_finding_raw", False),
    ("parathyroid_autograft_flag", True),
    ("gross_ete_flag", True),
    ("local_invasion_flag", True),
    ("tracheal_involvement_flag", True),
    ("esophageal_involvement_flag", True),
    ("strap_muscle_involvement_flag", True),
    ("reoperative_field_flag", True),
    ("drain_flag", True),
    ("operative_findings_raw", False),
    ("parathyroid_identified_count", False),
    ("frozen_section_flag", True),
    ("berry_ligament_flag", True),
    ("ebl_ml_nlp", False),
]

PATIENT_AGG_FIELDS = [
    ("op_rln_monitoring_any", True),
    ("op_drain_placed_any", True),
    ("op_strap_muscle_any", True),
    ("op_reoperative_any", True),
    ("op_parathyroid_autograft_any", True),
    ("op_local_invasion_any", True),
    ("op_tracheal_inv_any", True),
    ("op_esophageal_inv_any", True),
    ("op_intraop_gross_ete_any", True),
    ("op_n_surgeries_with_findings", False),
    ("op_findings_summary", False),
]

DOWNSTREAM_TABLES = [
    "episode_analysis_resolved_v1",
    "episode_analysis_resolved_v1_dedup",
]


def workstream_c(con) -> list[dict]:
    print("\n" + "=" * 78)
    print("  WORKSTREAM C — OPERATIVE NLP PROPAGATION")
    print("=" * 78)

    upstream_total = si(q1(con, "SELECT COUNT(*) FROM operative_episode_detail_v2"))
    results = []

    for field, is_bool in OPERATIVE_FIELDS:
        if is_bool:
            up_sql = f"SELECT SUM(CASE WHEN {field} IS TRUE THEN 1 ELSE 0 END) FROM operative_episode_detail_v2"
        elif "count" in field or "ebl" in field:
            up_sql = f"SELECT COUNT(*) FROM operative_episode_detail_v2 WHERE {field} IS NOT NULL AND {field} > 0"
        else:
            up_sql = f"SELECT COUNT(*) FROM operative_episode_detail_v2 WHERE {field} IS NOT NULL AND CAST({field} AS VARCHAR) != ''"
        up_val = si(q1(con, up_sql))
        up_pct = sp(up_val, upstream_total)

        # Check each downstream table
        down_vals = {}
        for dtbl in DOWNSTREAM_TABLES:
            try:
                dtotal = si(q1(con, f"SELECT COUNT(*) FROM {dtbl}"))
                if is_bool:
                    dsql = f"SELECT SUM(CASE WHEN {field} IS TRUE THEN 1 ELSE 0 END) FROM {dtbl}"
                elif "count" in field or "ebl" in field:
                    dsql = f"SELECT COUNT(*) FROM {dtbl} WHERE {field} IS NOT NULL AND {field} > 0"
                else:
                    dsql = f"SELECT COUNT(*) FROM {dtbl} WHERE {field} IS NOT NULL AND CAST({field} AS VARCHAR) != ''"
                dval = si(q1(con, dsql))
                dpct = sp(dval, dtotal)
                down_vals[dtbl] = (dval, dpct)
            except Exception:
                down_vals[dtbl] = (None, None)

        # Classify
        if up_val == 0:
            status = "SOURCE_LIMITED"
        elif all(v[0] == 0 or v[0] is None for v in down_vals.values()):
            status = "PIPELINE_GAP"
        elif all(v[0] is None for v in down_vals.values()):
            status = "NOT_PROPAGATED"
        else:
            status = "OK"

        entry = {"field": field, "upstream": up_val, "upstream_pct": up_pct,
                 "downstream": down_vals, "status": status}
        results.append(entry)
        dstr = "; ".join(f"{t}={v[0]}({v[1]}%)" for t, v in down_vals.items() if v[0] is not None)
        print(f"  {field}: upstream={up_val} ({up_pct}%) | {dstr} → {status}")

    # Patient-level aggregates
    print("\n  Patient-level aggregates:")
    pat_total = si(q1(con, "SELECT COUNT(*) FROM patient_analysis_resolved_v1"))
    for field, is_bool in PATIENT_AGG_FIELDS:
        if "summary" in field or "findings" in field:
            sql = f"SELECT COUNT(*) FROM patient_analysis_resolved_v1 WHERE {field} IS NOT NULL AND CAST({field} AS VARCHAR) != ''"
        elif "n_surgeries" in field:
            sql = f"SELECT COUNT(*) FROM patient_analysis_resolved_v1 WHERE {field} IS NOT NULL AND {field} > 0"
        elif is_bool:
            sql = f"SELECT SUM(CASE WHEN {field} IS TRUE THEN 1 ELSE 0 END) FROM patient_analysis_resolved_v1"
        else:
            sql = f"SELECT COUNT(*) FROM patient_analysis_resolved_v1 WHERE {field} IS NOT NULL"
        val = si(q1(con, sql))
        pct = sp(val, pat_total)
        results.append({"field": field, "upstream": val, "upstream_pct": pct,
                         "downstream": {}, "status": "patient_agg"})
        print(f"    {field}: {val} ({pct}%)")

    return results


# ═══════════════════════════════════════════════════════════════════════════════
# WORKSTREAM D — RECURRENCE REVIEW PACKETS
# ═══════════════════════════════════════════════════════════════════════════════

def workstream_d(con) -> dict:
    print("\n" + "=" * 78)
    print("  WORKSTREAM D — RECURRENCE REVIEW PACKETS")
    print("=" * 78)

    EXPORT_RECURRENCE.mkdir(parents=True, exist_ok=True)

    # Enhanced query: include multi-surgery context
    sql = """
    SELECT
        r.research_id,
        r.recurrence_any,
        r.recurrence_flag_structured,
        r.recurrence_date_status,
        r.recurrence_date_best,
        r.recurrence_site_inferred,
        r.detection_category,
        ps.surg_date AS first_surgery_date,
        ps.tumor_1_histologic_type AS histology,
        ps.tumor_1_size_greatest_dimension_cm AS tumor_size,
        ps.tumor_1_extrathyroidal_extension AS ete,
        COALESCE(ms.n_surgeries, 1) AS n_surgeries,
        COALESCE(tg.tg_last, -1) AS tg_last_value,
        COALESCE(tg.n_tg, 0) AS tg_measurements
    FROM extracted_recurrence_refined_v1 r
    LEFT JOIN (
        SELECT research_id, MIN(surg_date) AS surg_date,
               MAX(tumor_1_histologic_type) AS tumor_1_histologic_type,
               MAX(tumor_1_size_greatest_dimension_cm) AS tumor_1_size_greatest_dimension_cm,
               MAX(tumor_1_extrathyroidal_extension) AS tumor_1_extrathyroidal_extension
        FROM path_synoptics GROUP BY research_id
    ) ps ON r.research_id = ps.research_id
    LEFT JOIN (
        SELECT research_id, MAX(n_surgeries) AS n_surgeries
        FROM val_multi_surgery_cohort_v2
        GROUP BY research_id
    ) ms ON r.research_id = ms.research_id
    LEFT JOIN (
        SELECT research_id,
               MAX(TRY_CAST(result AS DOUBLE)) AS tg_last,
               COUNT(*) AS n_tg
        FROM thyroglobulin_labs
        WHERE result IS NOT NULL AND TRIM(result) != ''
        GROUP BY research_id
    ) tg ON r.research_id = tg.research_id
    WHERE r.recurrence_any IS TRUE
    ORDER BY
        CASE r.recurrence_date_status
            WHEN 'unresolved_date' THEN 1
            WHEN 'biochemical_inflection_inferred' THEN 2
            WHEN 'exact_source_date' THEN 3
            ELSE 4
        END,
        COALESCE(ms.n_surgeries, 1) DESC,
        r.research_id
    """

    rows = qdf(con, sql)
    if isinstance(rows, str):
        print(f"  ERROR: {rows}")
        return {"error": rows, "total": 0}

    total = len(rows)
    print(f"  Total recurrence cases: {total}")

    # Tier breakdown
    tier_counts = {}
    for r in rows:
        t = r.get("recurrence_date_status", "unknown")
        tier_counts[t] = tier_counts.get(t, 0) + 1
    for t, c in sorted(tier_counts.items(), key=lambda x: -x[1]):
        print(f"    {t}: {c}")

    # Multi-surgery recurrence patients
    ms_rec = sum(1 for r in rows if (r.get("n_surgeries") or 1) > 1)
    print(f"  Multi-surgery recurrence patients: {ms_rec}")

    # Export batches
    batch_size = 100
    n_batches = (total + batch_size - 1) // batch_size
    for i in range(n_batches):
        batch = rows[i * batch_size : (i + 1) * batch_size]
        if batch:
            fname = EXPORT_RECURRENCE / f"recurrence_review_batch_{i+1:03d}.csv"
            with open(fname, "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=batch[0].keys())
                w.writeheader()
                w.writerows(batch)

    manifest = {
        "generated": TIMESTAMP,
        "total_cases": total,
        "batches": n_batches,
        "tier_counts": tier_counts,
        "multi_surgery_recurrence_patients": ms_rec,
        "priority_order": "unresolved > biochemical > exact > other; multi-surgery first",
    }
    with open(EXPORT_RECURRENCE / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2, default=str)

    print(f"  Exported {n_batches} batches to {EXPORT_RECURRENCE}")
    return {"total": total, "tiers": tier_counts, "ms_recurrence": ms_rec, "batches": n_batches}


# ═══════════════════════════════════════════════════════════════════════════════
# WORKSTREAM E — DOCUMENTATION RECONCILIATION
# ═══════════════════════════════════════════════════════════════════════════════

def workstream_e(con, metrics: dict) -> list[dict]:
    print("\n" + "=" * 78)
    print("  WORKSTREAM E — DOCUMENTATION RECONCILIATION")
    print("=" * 78)

    mismatches = []
    live = {
        "patients": si(metrics.get("surgical_cohort", 0)),
        "cancer": si(metrics.get("analysis_cancer_cohort", 0)),
        "rai_total": si(metrics.get("rai_episodes", 0)),
        "rai_dose": si(metrics.get("rai_with_dose", 0)),
        "rec_unresolved": si(metrics.get("recurrence_unresolved", 0)),
        "rec_total": si(metrics.get("recurrence_flagged", 0)),
        "md_tables": si(metrics.get("md_table_count", 0)),
        "ms_patients": si(metrics.get("multi_surg_patients", 0)),
    }
    live["rai_pct"] = sp(live["rai_dose"], live["rai_total"])
    live["rec_pct"] = sp(live["rec_unresolved"], live["rec_total"])

    # Check each doc
    DOC_CHECKS = [
        ("README.md", [
            ("cohort size", r"10[,.]?871", str(live["patients"])),
            ("cancer cohort", r"4[,.]?136", str(live["cancer"])),
        ]),
        ("docs/MANUSCRIPT_CAVEATS_20260313.md", [
            ("RAI percentage", r"41%|41\.0%", f"{round(live['rai_pct'])}%"),
            ("recurrence unresolved", r"88\.8%", f"{live['rec_pct']}%"),
        ]),
        ("docs/SUPPLEMENT_DATA_QUALITY_APPENDIX_20260313.md", [
            ("total patients", r"10[,.]?871", str(live["patients"])),
        ]),
    ]

    for doc_path, checks in DOC_CHECKS:
        full_path = ROOT / doc_path
        if not full_path.exists():
            print(f"  {doc_path}: FILE NOT FOUND")
            mismatches.append({"doc": doc_path, "metric": "file", "documented": "N/A", "live": "MISSING"})
            continue

        text = full_path.read_text()
        for label, pattern, live_val in checks:
            found = re.search(pattern, text)
            if found:
                doc_val = found.group(0)
                clean_doc = doc_val.replace(",", "").replace("%", "")
                clean_live = live_val.replace(",", "").replace("%", "")
                match = clean_doc == clean_live
                status = "MATCH" if match else "MISMATCH"
                if not match:
                    mismatches.append({"doc": doc_path, "metric": label, "documented": doc_val, "live": live_val})
            else:
                doc_val = "NOT_FOUND"
                status = "NOT_FOUND"
            print(f"  {doc_path} | {label}: doc='{doc_val}', live='{live_val}' → {status}")

    # Check README MotherDuck table count
    readme_path = ROOT / "README.md"
    if readme_path.exists():
        text = readme_path.read_text()
        md_match = re.search(r'(\d+)\s*(?:MotherDuck|motherduck)\s*(?:tables|views)', text, re.IGNORECASE)
        if md_match:
            doc_n = int(md_match.group(1))
            if doc_n != live["md_tables"]:
                mismatches.append({"doc": "README.md", "metric": "MotherDuck tables",
                                   "documented": str(doc_n), "live": str(live["md_tables"])})
                print(f"  README.md | MotherDuck tables: doc='{doc_n}', live='{live['md_tables']}' → MISMATCH")
            else:
                print(f"  README.md | MotherDuck tables: doc='{doc_n}', live='{live['md_tables']}' → MATCH")

    return mismatches


# ═══════════════════════════════════════════════════════════════════════════════
# OUTPUT GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

def write_truth_snapshot(metrics, op_audit, rec_info, linkage, mismatches):
    """Write docs/dataset_truth_snapshot_YYYYMMDD.md"""
    path = DOCS / f"dataset_truth_snapshot_{DATESTAMP}.md"

    lines = [
        f"# Dataset Truth Snapshot — {DATESTAMP}",
        "",
        f"Generated: {NOW.isoformat()}",
        f"Source: MotherDuck `thyroid_research_2026` (prod)",
        f"Script: `scripts/98_final_verification_pass.py`",
        "",
        "## 1. Core Dataset Metrics",
        "",
        "| Metric | Value |",
        "|--------|-------|",
    ]
    for k, v in metrics.items():
        label = k.replace("_", " ").title()
        lines.append(f"| {label} | {v} |")

    # Multi-surgery section
    lines += [
        "",
        "## 2. Multi-Surgery Episode Integrity",
        "",
        f"- Multi-surgery patients: **{linkage.get('cohort_patients', 'N/A')}**",
        f"- Multi-surgery episodes: **{linkage.get('cohort_episodes', 'N/A')}**",
        f"- High-risk review queue: **{linkage.get('review_queue_rows', 'N/A')}** patients",
        "",
        "### Artifact Linkage by Domain",
        "",
        "| Domain | MS Artifacts | Uniquely Linked | Ambiguous | Distant | Unlinked |",
        "|--------|-------------|----------------|-----------|---------|----------|",
    ]
    for d in linkage.get("domains", []):
        lines.append(f"| {d['domain']} | {d.get('total_ms_artifacts', 0)} | {d.get('uniquely_linked', 0)} | {d.get('ambiguous', 0)} | {d.get('distant_linked', 0)} | {d.get('unlinked', 0)} |")

    lines += [
        "",
        "### Episode Key Propagation",
        "",
        "| Table | MS Rows | Distinct IDs | Status |",
        "|-------|---------|-------------|--------|",
    ]
    for p in linkage.get("propagation", []):
        lines.append(f"| {p['table']} | {p.get('ms_rows', 'N/A')} | {p.get('distinct_episode_ids', 'N/A')} | {p['status']} |")

    # Recurrence
    lines += [
        "",
        "## 3. Recurrence Resolution Tiers",
        "",
        "| Tier | Count |",
        "|------|-------|",
    ]
    for t, c in sorted(rec_info.get("tiers", {}).items(), key=lambda x: -x[1]):
        lines.append(f"| {t} | {c} |")
    lines.append(f"")
    lines.append(f"Multi-surgery recurrence patients: {rec_info.get('ms_recurrence', 0)}")
    lines.append(f"Recurrence review packets: {rec_info.get('total', 0)} cases → `exports/recurrence_review_packets/`")

    # RAI
    rai_t = si(metrics.get("rai_episodes", 0))
    rai_d = si(metrics.get("rai_with_dose", 0))
    lines += [
        "",
        "## 4. RAI Dose Coverage",
        "",
        f"- Episodes: {rai_t}",
        f"- With dose: {rai_d}",
        f"- Coverage: **{sp(rai_d, rai_t)}%**",
    ]

    # Operative NLP
    lines += [
        "",
        "## 5. Operative NLP Field Coverage",
        "",
        "| Field | Upstream | Upstream % | Status |",
        "|-------|----------|-----------|--------|",
    ]
    for r in op_audit:
        if r["status"] != "patient_agg":
            lines.append(f"| {r['field']} | {r['upstream']} | {r['upstream_pct']}% | {r['status']} |")
    lines += ["", "### Patient-Level Aggregates", "",
              "| Field | Count | % |", "|-------|-------|---|"]
    for r in op_audit:
        if r["status"] == "patient_agg":
            lines.append(f"| {r['field']} | {r['upstream']} | {r['upstream_pct']}% |")

    # Labs
    lines += [
        "",
        "## 6. Lab Coverage",
        "",
        f"- Canonical lab rows: {metrics.get('lab_canonical_rows', 'N/A')}",
        f"- Canonical lab patients: {metrics.get('lab_canonical_patients', 'N/A')}",
        f"- Tg lab patients: {metrics.get('tg_patients', 'N/A')}",
    ]

    # Imaging
    lines += [
        "",
        "## 7. Imaging & Linkage",
        "",
        f"- Imaging nodule rows: {metrics.get('imaging_nodule_rows', 'N/A')}",
        f"- TIRADS patients: {metrics.get('tirads_patients', 'N/A')}",
        f"- Imaging-FNA linkage: {metrics.get('imaging_fna_linkage', 'N/A')}",
    ]

    # Adjudication
    lines += [
        "",
        "## 8. Adjudication",
        "",
        f"- Adjudication decisions: {metrics.get('adjudication_decisions', 'N/A')}",
        f"- Complications refined: {metrics.get('complications_refined', 'N/A')}",
        f"- Complication patients: {metrics.get('complications_patients', 'N/A')}",
    ]

    # Doc reconciliation
    lines += [
        "",
        "## 9. Documentation Reconciliation",
        "",
    ]
    if mismatches:
        lines += ["| Doc | Metric | Documented | Live |",
                   "|-----|--------|-----------|------|"]
        for m in mismatches:
            lines.append(f"| {m['doc']} | {m['metric']} | {m['documented']} | {m['live']} |")
    else:
        lines.append("All documentation numbers match live MotherDuck. No updates needed.")

    # Source-limited
    lines += [
        "",
        "## 10. Source-Limited Fields",
        "",
        "| Field | Coverage | Limitation |",
        "|-------|---------|------------|",
        "| Non-Tg lab dates (TSH/PTH/Ca) | 0% | Institutional lab extract needed |",
        "| Nuclear medicine notes | 0 | Not in clinical_notes_long |",
        "| Vascular invasion grading | 87% ungraded | Synoptic 'x' placeholder |",
        "| Recurrence dates | ~89% unresolved | Manual chart review needed |",
        "| Esophageal involvement | 0% | No NLP entities extracted |",
        "| Frozen section / Berry ligament | 0% | Entity type not in NLP vocab |",
        "",
        "---",
        f"*Generated by `scripts/98_final_verification_pass.py` on {NOW.isoformat()}*",
    ]

    path.write_text("\n".join(lines))
    print(f"\n✓ Truth snapshot: {path}")
    return path


def write_linkage_audit(linkage):
    """Write docs/multi_surgery_linkage_audit_YYYYMMDD.md"""
    path = DOCS / f"multi_surgery_linkage_audit_{DATESTAMP}.md"
    lines = [
        f"# Multi-Surgery Episode Linkage Audit — {DATESTAMP}",
        "",
        f"Generated: {NOW.isoformat()}",
        f"Source: MotherDuck `thyroid_research_2026` (prod)",
        "",
        "## Summary",
        "",
        f"- Multi-surgery patients: **{linkage.get('cohort_patients', 'N/A')}**",
        f"- Total surgery episodes: **{linkage.get('cohort_episodes', 'N/A')}**",
        f"- High-risk review queue: **{linkage.get('review_queue_rows', 'N/A')}**",
        "",
        "## Artifact Assignment by Domain",
        "",
        "| Domain | Table | MS Artifacts | Uniquely Linked | Ambiguous | Distant | Unlinked |",
        "|--------|-------|-------------|----------------|-----------|---------|----------|",
    ]
    for d in linkage.get("domains", []):
        lines.append(
            f"| {d['domain']} | {d.get('table', '')} | {d.get('total_ms_artifacts', 0)} | "
            f"{d.get('uniquely_linked', 0)} | {d.get('ambiguous', 0)} | "
            f"{d.get('distant_linked', 0)} | {d.get('unlinked', 0)} |"
        )

    lines += [
        "",
        "## Episode Key Propagation",
        "",
        "| Table | Column | MS Rows | Distinct Episodes | Status |",
        "|-------|--------|---------|------------------|--------|",
    ]
    for p in linkage.get("propagation", []):
        lines.append(f"| {p['table']} | {p['column']} | {p.get('ms_rows','N/A')} | {p.get('distinct_episode_ids','N/A')} | {p['status']} |")

    lines += [
        "",
        "## MotherDuck Objects Created",
        "",
        "| Table | Purpose |",
        "|-------|---------|",
        "| `val_multi_surgery_cohort_v2` | Per-episode cohort (all multi-surgery patients) |",
        "| `val_multi_surgery_review_queue_v2` | High-risk cases for manual review |",
        "",
        "## Interpretation",
        "",
        "- *Uniquely linked*: artifact date falls within 30 days of exactly one surgery, with >14-day gap to next-nearest",
        "- *Ambiguous*: equidistant (≤14-day difference) between two surgeries",
        "- *Distant linked*: 30-365 days from nearest surgery — follow-up or temporal gap",
        "- *Unlinked*: >365 days from any surgery — long-term surveillance, not a linkage failure",
        "",
        "---",
        f"*Audit generated by `scripts/98_final_verification_pass.py` on {NOW.isoformat()}*",
    ]
    path.write_text("\n".join(lines))
    print(f"✓ Linkage audit: {path}")
    return path


def write_episode_rulebook():
    """Write docs/episode_linkage_rulebook_YYYYMMDD.md"""
    path = DOCS / f"episode_linkage_rulebook_{DATESTAMP}.md"
    lines = [
        f"# Episode Linkage Rulebook — {DATESTAMP}",
        "",
        f"Generated: {NOW.isoformat()}",
        "",
        "## 1. Surgery Episode Assignment",
        "",
        "Each patient's surgeries are ordered chronologically. `surgery_episode_id` is",
        "assigned as ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY surg_date).",
        "",
        "## 2. Artifact → Surgery Assignment Rules",
        "",
        "| Domain | Rule | Window | Tiebreaker |",
        "|--------|------|--------|-----------|",
        "| Pathology | Same surg_date | 0 days | Exact match required |",
        "| Operative | Same surgery_date_native | 0 days | Exact match required |",
        "| FNA | Nearest surgery ≤180 days before | -7 to 180d | Temporal proximity |",
        "| Molecular | Nearest FNA-linked surgery | Via FNA linkage | FNA→surgery chain |",
        "| RAI | Nearest preceding surgery ≤365d | 0 to 365d post | Temporal proximity |",
        "| Labs (Tg) | Midpoint between surgeries | Midpoint bisector | None — assign to closer |",
        "| Imaging (US) | Nearest surgery ≤365d before | -365 to 0d | Temporal proximity |",
        "",
        "## 3. Ambiguity Rules",
        "",
        "- If artifact date falls within 14 days of the midpoint between two surgeries: **ambiguous**",
        "- Ambiguous artifacts are flagged but assigned to the temporally closer surgery",
        "- Labs drawn between two surgeries with <14-day difference to midpoint: mark for manual review",
        "",
        "## 4. Mislink Detection",
        "",
        "- V3 linkage tables may carry stale `surgery_episode_id` from initial build",
        "- Mislink = linked surgery_episode_id's date differs from artifact date's nearest surgery",
        "- Minor mismatch = date difference <7 days (recording delay)",
        "",
        "## 5. Confidence Tiers",
        "",
        "| Tier | Definition | Window |",
        "|------|-----------|--------|",
        "| exact_match | Art date = surgery date | 0 days |",
        "| high_confidence | ≤30 days and >14-day gap to next surgery | 0-30d |",
        "| plausible | 30-365 days from nearest | 30-365d |",
        "| weak | >365 days but closest | >365d |",
        "| unlinked | No surgery within 365 days | N/A |",
        "",
        "---",
        f"*Rulebook generated by `scripts/98_final_verification_pass.py` on {NOW.isoformat()}*",
    ]
    path.write_text("\n".join(lines))
    print(f"✓ Episode rulebook: {path}")
    return path


def write_operative_audit(op_audit):
    """Write docs/operative_nlp_propagation_audit_YYYYMMDD.md"""
    path = DOCS / f"operative_nlp_propagation_audit_{DATESTAMP}.md"
    ep_fields = [r for r in op_audit if r["status"] != "patient_agg"]
    pat_fields = [r for r in op_audit if r["status"] == "patient_agg"]
    ok = [r for r in ep_fields if r["status"] == "OK"]
    gaps = [r for r in ep_fields if r["status"] == "PIPELINE_GAP"]
    src_lim = [r for r in ep_fields if r["status"] == "SOURCE_LIMITED"]
    not_prop = [r for r in ep_fields if r["status"] == "NOT_PROPAGATED"]

    lines = [
        f"# Operative NLP Propagation Audit — {DATESTAMP}",
        "",
        f"Generated: {NOW.isoformat()}",
        "",
        "## Summary",
        "",
        f"- Fully propagated: **{len(ok)}** fields",
        f"- Pipeline gap (upstream present, downstream absent): **{len(gaps)}** fields",
        f"- Source-limited (0% upstream): **{len(src_lim)}** fields",
        f"- Not propagated (no downstream target): **{len(not_prop)}** fields",
        "",
        "## Episode-Level Fields",
        "",
        "| Field | Upstream | % | Status |",
        "|-------|----------|---|--------|",
    ]
    for r in ep_fields:
        lines.append(f"| `{r['field']}` | {r['upstream']} | {r['upstream_pct']}% | {r['status']} |")

    lines += ["", "## Patient-Level Aggregates", "",
              "| Field | Count | % |", "|-------|-------|---|"]
    for r in pat_fields:
        lines.append(f"| `{r['field']}` | {r['upstream']} | {r['upstream_pct']}% |")

    lines += [
        "",
        "## Classification Detail",
        "",
        f"### OK ({len(ok)})", "",
    ]
    for r in ok:
        lines.append(f"- `{r['field']}`")
    lines += [f"", f"### Pipeline Gap ({len(gaps)})", ""]
    for r in gaps:
        lines.append(f"- `{r['field']}`: present in operative_episode_detail_v2 but missing from episode tables")
    lines += [f"", f"### Source-Limited ({len(src_lim)})", ""]
    for r in src_lim:
        lines.append(f"- `{r['field']}`: 0% in upstream extraction")
    lines += [f"", f"### Not Propagated ({len(not_prop)})", ""]
    for r in not_prop:
        lines.append(f"- `{r['field']}`: no downstream table column exists")

    lines += [
        "",
        "---",
        f"*Audit generated by `scripts/98_final_verification_pass.py` on {NOW.isoformat()}*",
    ]
    path.write_text("\n".join(lines))
    print(f"✓ Operative NLP audit: {path}")
    return path


def write_recurrence_status(rec_info):
    """Write docs/recurrence_review_status_YYYYMMDD.md"""
    path = DOCS / f"recurrence_review_status_{DATESTAMP}.md"
    lines = [
        f"# Recurrence Review Status — {DATESTAMP}",
        "",
        f"Generated: {NOW.isoformat()}",
        "",
        "## Case Summary",
        "",
        f"- Total recurrence cases: **{rec_info.get('total', 0)}**",
        f"- Multi-surgery recurrences: **{rec_info.get('ms_recurrence', 0)}**",
        f"- Review batches exported: **{rec_info.get('batches', 0)}**",
        "",
        "## Resolution Tier Distribution",
        "",
        "| Tier | Count | % |",
        "|------|-------|---|",
    ]
    total = rec_info.get("total", 0)
    for t, c in sorted(rec_info.get("tiers", {}).items(), key=lambda x: -x[1]):
        lines.append(f"| {t} | {c} | {sp(c, total)}% |")

    lines += [
        "",
        "## Export Location",
        "",
        f"- Directory: `exports/recurrence_review_packets/`",
        f"- Format: CSV batches of 100, priority-sorted",
        f"- Priority: unresolved > biochemical > exact; multi-surgery patients first within tier",
        "",
        "## Multi-Surgery Context",
        "",
        "Recurrence review packets now include `n_surgeries` column to flag multi-surgery",
        "patients who may need episode-level adjudication of recurrence timing.",
        "",
        "---",
        f"*Status report generated by `scripts/98_final_verification_pass.py` on {NOW.isoformat()}*",
    ]
    path.write_text("\n".join(lines))
    print(f"✓ Recurrence status: {path}")
    return path


def write_exports(metrics, op_audit, linkage, rec_info, mismatches):
    """Write CSV/JSON exports."""
    EXPORT_TRUTH_DIR.mkdir(parents=True, exist_ok=True)

    # Metrics CSV
    with open(EXPORT_TRUTH_DIR / "dataset_metrics.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        for k, v in metrics.items():
            w.writerow([k, v])

    # Operative audit CSV
    with open(EXPORT_TRUTH_DIR / "operative_nlp_audit.csv", "w", newline="") as f:
        flat = []
        for r in op_audit:
            flat.append({
                "field": r["field"],
                "upstream": r["upstream"],
                "upstream_pct": r["upstream_pct"],
                "status": r["status"],
            })
        if flat:
            w = csv.DictWriter(f, fieldnames=flat[0].keys())
            w.writeheader()
            w.writerows(flat)

    # Linkage domain CSV
    domains = linkage.get("domains", [])
    if domains:
        with open(EXPORT_TRUTH_DIR / "linkage_by_domain.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=domains[0].keys())
            w.writeheader()
            w.writerows(domains)

    # Doc mismatch CSV
    if mismatches:
        with open(EXPORT_TRUTH_DIR / "doc_mismatches.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=mismatches[0].keys())
            w.writeheader()
            w.writerows(mismatches)

    # Manifest
    manifest = {
        "generated": TIMESTAMP,
        "source": "MotherDuck thyroid_research_2026 (prod)",
        "script": "scripts/98_final_verification_pass.py",
        "workstreams": ["A_truth_snapshot", "B_multi_surgery_linkage",
                        "C_operative_nlp", "D_recurrence_packets", "E_doc_reconciliation"],
        "metrics_count": len(metrics),
        "operative_fields_audited": len(op_audit),
        "recurrence_cases": rec_info.get("total", 0),
        "doc_mismatches": len(mismatches),
        "linkage_domains": len(domains),
        "files": [
            "dataset_metrics.csv",
            "operative_nlp_audit.csv",
            "linkage_by_domain.csv",
            "doc_mismatches.csv",
            "manifest.json",
        ],
    }
    with open(EXPORT_TRUTH_DIR / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2, default=str)

    # Also export linkage audit
    EXPORT_LINKAGE_DIR.mkdir(parents=True, exist_ok=True)
    if domains:
        with open(EXPORT_LINKAGE_DIR / "artifact_linkage_by_domain.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=domains[0].keys())
            w.writeheader()
            w.writerows(domains)
    props = linkage.get("propagation", [])
    if props:
        with open(EXPORT_LINKAGE_DIR / "episode_key_propagation.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=props[0].keys())
            w.writeheader()
            w.writerows(props)
    link_manifest = {
        "generated": TIMESTAMP,
        "source": "MotherDuck thyroid_research_2026 (prod)",
        "ms_patients": linkage.get("cohort_patients", 0),
        "ms_episodes": linkage.get("cohort_episodes", 0),
        "review_queue": linkage.get("review_queue_rows", 0),
    }
    with open(EXPORT_LINKAGE_DIR / "manifest.json", "w") as f:
        json.dump(link_manifest, f, indent=2, default=str)

    print(f"\n✓ Truth exports: {EXPORT_TRUTH_DIR}")
    print(f"✓ Linkage exports: {EXPORT_LINKAGE_DIR}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Final comprehensive verification pass")
    parser.add_argument("--md", action="store_true", default=True, help="Use MotherDuck (default)")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 78)
    print("  FINAL COMPREHENSIVE VERIFICATION PASS")
    print(f"  {NOW.isoformat()}")
    print("=" * 78)

    if args.local:
        print("ERROR: This script requires MotherDuck for production verification.")
        sys.exit(1)

    con = connect_md()

    # A — Truth snapshot
    metrics = workstream_a(con)

    # B — Multi-surgery linkage
    linkage = workstream_b(con, metrics)

    # C — Operative NLP
    op_audit = workstream_c(con)

    # D — Recurrence review
    rec_info = workstream_d(con)

    # E — Doc reconciliation
    mismatches = workstream_e(con, metrics)

    # Write all deliverables
    print("\n" + "=" * 78)
    print("  WRITING DELIVERABLES")
    print("=" * 78)

    write_truth_snapshot(metrics, op_audit, rec_info, linkage, mismatches)
    write_linkage_audit(linkage)
    write_episode_rulebook()
    write_operative_audit(op_audit)
    write_recurrence_status(rec_info)
    write_exports(metrics, op_audit, linkage, rec_info, mismatches)

    # Final summary
    print("\n" + "=" * 78)
    print("  VERIFICATION COMPLETE")
    print("=" * 78)
    print(f"  Metrics computed:       {len(metrics)}")
    print(f"  Multi-surgery patients: {linkage.get('cohort_patients', 'N/A')}")
    print(f"  Multi-surgery episodes: {linkage.get('cohort_episodes', 'N/A')}")
    print(f"  Operative fields:       {len(op_audit)}")
    print(f"  Recurrence cases:       {rec_info.get('total', 'N/A')}")
    print(f"  Doc mismatches:         {len(mismatches)}")
    print(f"  MotherDuck tables created: val_multi_surgery_cohort_v2, val_multi_surgery_review_queue_v2")

    con.close()
    return 0 if len(mismatches) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
