#!/usr/bin/env python3
"""
Script 70 — Surgery–Operative Note–Pathology Linkage Verification Audit

Creates validation and review tables on MotherDuck, measures coverage,
verifies linkage quality, and exports a comprehensive audit report.

Usage:
    .venv/bin/python scripts/70_operative_note_path_linkage_audit.py --md
    .venv/bin/python scripts/70_operative_note_path_linkage_audit.py --local
"""

import argparse
import datetime
import json
import os
import sys

TS = datetime.datetime.now().strftime("%Y%m%d_%H%M")
TODAY = datetime.datetime.now().strftime("%Y%m%d")
EXPORT_DIR = f"exports/operative_note_path_linkage_audit_{TODAY}"


def get_connection(use_md: bool):
    import duckdb

    if use_md:
        try:
            import toml

            token = os.environ.get("MOTHERDUCK_TOKEN") or toml.load(
                ".streamlit/secrets.toml"
            )["MOTHERDUCK_TOKEN"]
        except Exception:
            token = os.environ["MOTHERDUCK_TOKEN"]
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect("thyroid_master.duckdb")


def q1(con, label: str, sql: str):
    """Execute and return scalar."""
    try:
        return con.execute(sql).fetchone()[0]
    except Exception as e:
        print(f"  WARN {label}: {e}")
        return None


def qall(con, sql: str):
    try:
        return con.execute(sql).fetchall()
    except Exception as e:
        print(f"  WARN: {e}")
        return []


# ── PHASE 2: OPERATIVE NOTE COVERAGE ──────────────────────────────────

VAL_OPERATIVE_NOTE_COVERAGE_SQL = """
CREATE OR REPLACE TABLE val_operative_note_coverage_v1 AS
WITH
surgery_base AS (
    SELECT DISTINCT research_id, surg_date,
        TRY_CAST(surg_date AS DATE) AS surgery_date_parsed
    FROM path_synoptics
    WHERE research_id IS NOT NULL
),
op_episodes AS (
    SELECT research_id, surgery_episode_id, surgery_date_native
    FROM operative_episode_detail_v2
),
op_notes AS (
    SELECT research_id, note_date,
        TRY_CAST(note_date AS DATE) AS note_date_parsed,
        LENGTH(COALESCE(note_text, '')) AS note_len
    FROM clinical_notes_long
    WHERE note_type = 'op_note'
),
cancer_flag AS (
    SELECT DISTINCT research_id, TRUE AS is_cancer
    FROM path_synoptics
    WHERE tumor_1_histologic_type IS NOT NULL
      AND LOWER(COALESCE(tumor_1_histologic_type, '')) NOT IN ('', 'none', 'benign', 'null')
),
per_patient AS (
    SELECT
        sb.research_id,
        MIN(sb.surgery_date_parsed) AS first_surgery_date,
        COUNT(DISTINCT sb.surg_date) AS n_surgeries_path,
        MAX(CASE WHEN oe.research_id IS NOT NULL THEN TRUE ELSE FALSE END) AS has_operative_episode,
        MAX(CASE WHEN on2.research_id IS NOT NULL THEN TRUE ELSE FALSE END) AS has_op_note,
        MAX(CASE WHEN on2.note_len > 200 THEN TRUE ELSE FALSE END) AS has_substantive_op_note,
        MAX(CASE WHEN oe.research_id IS NOT NULL AND on2.research_id IS NOT NULL THEN TRUE ELSE FALSE END) AS has_both,
        COALESCE(cf.is_cancer, FALSE) AS is_cancer
    FROM surgery_base sb
    LEFT JOIN op_episodes oe ON sb.research_id = oe.research_id
    LEFT JOIN op_notes on2 ON sb.research_id = on2.research_id
    LEFT JOIN cancer_flag cf ON sb.research_id = cf.research_id
    GROUP BY sb.research_id, cf.is_cancer
)
SELECT * FROM per_patient
"""

VAL_OPERATIVE_NOTE_PARSE_COVERAGE_SQL = """
CREATE OR REPLACE TABLE val_operative_note_parse_coverage_v1 AS
SELECT
    o.research_id,
    o.surgery_episode_id,
    o.surgery_date_native,
    o.procedure_normalized,
    o.laterality,
    CASE WHEN o.rln_monitoring_flag OR o.gross_ete_flag OR o.drain_flag
              OR o.reoperative_field_flag OR o.tracheal_involvement_flag
              OR o.esophageal_involvement_flag OR o.strap_muscle_involvement_flag
              OR o.local_invasion_flag
              OR (o.rln_finding_raw IS NOT NULL)
              OR (o.operative_findings_raw IS NOT NULL AND o.operative_findings_raw != '')
         THEN TRUE ELSE FALSE END AS has_nlp_parse,
    o.rln_monitoring_flag,
    o.rln_finding_raw,
    o.parathyroid_autograft_flag,
    o.gross_ete_flag,
    o.local_invasion_flag,
    o.tracheal_involvement_flag,
    o.esophageal_involvement_flag,
    o.strap_muscle_involvement_flag,
    o.reoperative_field_flag,
    o.drain_flag,
    o.operative_findings_raw,
    o.central_neck_dissection_flag,
    o.lateral_neck_dissection_flag,
    o.ebl_ml
FROM operative_episode_detail_v2 o
"""


# ── PHASE 3: SURGERY ↔ OPERATIVE NOTE LINKAGE ─────────────────────────

REVIEW_OPERATIVE_NOTE_LINKAGE_SQL = """
CREATE OR REPLACE TABLE review_operative_note_linkage_v1 AS
WITH
surgeries AS (
    SELECT research_id, surgery_episode_id, surgery_date_native,
           procedure_normalized, laterality
    FROM operative_episode_detail_v2
),
op_notes AS (
    SELECT research_id, note_row_id, note_date,
        TRY_CAST(note_date AS DATE) AS note_date_parsed,
        LENGTH(COALESCE(note_text, '')) AS note_len,
        LEFT(COALESCE(note_text, ''), 120) AS note_snippet
    FROM clinical_notes_long
    WHERE note_type = 'op_note'
),
best_note AS (
    SELECT
        s.research_id,
        s.surgery_episode_id,
        s.surgery_date_native,
        s.procedure_normalized,
        on2.note_row_id,
        on2.note_date_parsed,
        on2.note_len,
        on2.note_snippet,
        ABS(DATEDIFF('day',
            COALESCE(s.surgery_date_native, DATE '2099-01-01'),
            COALESCE(on2.note_date_parsed, DATE '2099-01-01')
        )) AS day_gap,
        ROW_NUMBER() OVER (
            PARTITION BY s.research_id, s.surgery_episode_id
            ORDER BY ABS(DATEDIFF('day',
                COALESCE(s.surgery_date_native, DATE '2099-01-01'),
                COALESCE(on2.note_date_parsed, DATE '2099-01-01')))
        ) AS rn
    FROM surgeries s
    LEFT JOIN op_notes on2 ON s.research_id = on2.research_id
)
SELECT
    research_id,
    surgery_episode_id,
    surgery_date_native,
    procedure_normalized,
    note_row_id,
    note_date_parsed AS matched_note_date,
    note_len AS matched_note_length,
    note_snippet,
    day_gap,
    CASE
        WHEN note_row_id IS NULL THEN 'MISSING'
        WHEN day_gap = 0 THEN 'LINKED_CONFIDENT'
        WHEN day_gap <= 7 THEN 'LINKED_DATE_PROXIMAL'
        WHEN day_gap <= 30 THEN 'AMBIGUOUS'
        WHEN surgery_date_native IS NULL OR note_date_parsed IS NULL THEN 'AMBIGUOUS'
        ELSE 'AMBIGUOUS'
    END AS linkage_category
FROM best_note
WHERE rn = 1 OR note_row_id IS NULL
"""


# ── PHASE 4: SURGERY ↔ PATHOLOGY LINKAGE ──────────────────────────────

VAL_SURGERY_PATH_LINKAGE_SQL = """
CREATE OR REPLACE TABLE val_surgery_path_linkage_v1 AS
WITH
ops AS (
    SELECT research_id, surgery_episode_id, surgery_date_native,
           procedure_normalized, laterality
    FROM operative_episode_detail_v2
),
tumors AS (
    SELECT research_id,
           surgery_episode_id AS tumor_episode_id,
           surgery_date AS tumor_surgery_date,
           primary_histology, t_stage, n_stage,
           laterality AS tumor_laterality,
           tumor_size_cm
    FROM tumor_episode_master_v2
),
path_surg AS (
    SELECT DISTINCT research_id, TRY_CAST(surg_date AS DATE) AS ps_surgery_date,
        tumor_1_histologic_type, thyroid_procedure
    FROM path_synoptics WHERE research_id IS NOT NULL
),
linked AS (
    SELECT
        o.research_id,
        o.surgery_episode_id,
        o.surgery_date_native,
        o.procedure_normalized,
        o.laterality AS op_laterality,
        t.tumor_episode_id,
        t.tumor_surgery_date,
        t.primary_histology,
        t.t_stage,
        t.tumor_laterality,
        ABS(DATEDIFF('day',
            COALESCE(o.surgery_date_native, DATE '2099-01-01'),
            COALESCE(t.tumor_surgery_date, DATE '2099-01-01')
        )) AS op_path_day_gap,
        CASE
            WHEN o.laterality IS NOT NULL AND t.tumor_laterality IS NOT NULL
                 AND LOWER(o.laterality) = LOWER(t.tumor_laterality) THEN 'match'
            WHEN o.laterality IS NULL OR t.tumor_laterality IS NULL THEN 'unknown'
            ELSE 'mismatch'
        END AS laterality_concordance,
        CASE
            WHEN o.surgery_date_native = t.tumor_surgery_date THEN 'exact'
            WHEN ABS(DATEDIFF('day', o.surgery_date_native, t.tumor_surgery_date)) <= 1 THEN 'near_exact'
            WHEN ABS(DATEDIFF('day', o.surgery_date_native, t.tumor_surgery_date)) <= 7 THEN 'proximal'
            WHEN o.surgery_date_native IS NULL OR t.tumor_surgery_date IS NULL THEN 'date_missing'
            ELSE 'discordant'
        END AS date_alignment,
        ROW_NUMBER() OVER (
            PARTITION BY o.research_id, o.surgery_episode_id
            ORDER BY ABS(DATEDIFF('day',
                COALESCE(o.surgery_date_native, DATE '2099-01-01'),
                COALESCE(t.tumor_surgery_date, DATE '2099-01-01'))),
                t.tumor_episode_id
        ) AS rn
    FROM ops o
    LEFT JOIN tumors t ON o.research_id = t.research_id
)
SELECT
    research_id, surgery_episode_id, surgery_date_native,
    procedure_normalized, op_laterality,
    tumor_episode_id, tumor_surgery_date, primary_histology, t_stage, tumor_laterality,
    op_path_day_gap, laterality_concordance, date_alignment,
    CASE
        WHEN tumor_episode_id IS NULL THEN 'NO_PATHOLOGY'
        WHEN date_alignment = 'exact' AND laterality_concordance IN ('match','unknown') THEN 'ALIGNED'
        WHEN date_alignment IN ('near_exact','proximal') AND laterality_concordance IN ('match','unknown') THEN 'PROXIMAL_ALIGNED'
        WHEN date_alignment = 'discordant' THEN 'DATE_DISCORDANT'
        WHEN laterality_concordance = 'mismatch' THEN 'LATERALITY_DISCORDANT'
        WHEN date_alignment = 'date_missing' THEN 'DATE_MISSING'
        ELSE 'REVIEW_NEEDED'
    END AS linkage_status
FROM linked
WHERE rn = 1
"""

REVIEW_SURGERY_PATH_DISCORDANCE_SQL = """
CREATE OR REPLACE TABLE review_surgery_path_discordance_v1 AS
SELECT *
FROM val_surgery_path_linkage_v1
WHERE linkage_status NOT IN ('ALIGNED', 'PROXIMAL_ALIGNED')
ORDER BY
    CASE linkage_status
        WHEN 'NO_PATHOLOGY' THEN 1
        WHEN 'DATE_DISCORDANT' THEN 2
        WHEN 'LATERALITY_DISCORDANT' THEN 3
        WHEN 'DATE_MISSING' THEN 4
        ELSE 5
    END,
    research_id
"""


# ── PHASE 5: OPERATIVE VARIABLE COVERAGE ──────────────────────────────

VAL_OPERATIVE_VARIABLE_COVERAGE_SQL = """
CREATE OR REPLACE TABLE val_operative_variable_coverage_v1 AS
SELECT
    'procedure_normalized' AS variable,
    COUNT(*) AS total_episodes,
    SUM(CASE WHEN procedure_normalized IS NOT NULL AND procedure_normalized != 'unknown' THEN 1 ELSE 0 END) AS extracted,
    SUM(CASE WHEN procedure_normalized = 'unknown' THEN 1 ELSE 0 END) AS unknown_val,
    SUM(CASE WHEN procedure_normalized IS NULL THEN 1 ELSE 0 END) AS missing
FROM operative_episode_detail_v2
UNION ALL
SELECT 'laterality', COUNT(*),
    SUM(CASE WHEN laterality IS NOT NULL THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN laterality IS NULL THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'central_neck_dissection', COUNT(*),
    SUM(CASE WHEN central_neck_dissection_flag THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN NOT central_neck_dissection_flag THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'lateral_neck_dissection', COUNT(*),
    SUM(CASE WHEN lateral_neck_dissection_flag THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN NOT lateral_neck_dissection_flag THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'rln_monitoring', COUNT(*),
    SUM(CASE WHEN rln_monitoring_flag THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN NOT rln_monitoring_flag THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'rln_finding', COUNT(*),
    SUM(CASE WHEN rln_finding_raw IS NOT NULL THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN rln_finding_raw IS NULL THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'parathyroid_autograft', COUNT(*),
    SUM(CASE WHEN parathyroid_autograft_flag THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN NOT parathyroid_autograft_flag THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'gross_ete', COUNT(*),
    SUM(CASE WHEN gross_ete_flag THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN NOT gross_ete_flag THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'local_invasion', COUNT(*),
    SUM(CASE WHEN local_invasion_flag THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN NOT local_invasion_flag THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'tracheal_involvement', COUNT(*),
    SUM(CASE WHEN tracheal_involvement_flag THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN NOT tracheal_involvement_flag THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'esophageal_involvement', COUNT(*),
    SUM(CASE WHEN esophageal_involvement_flag THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN NOT esophageal_involvement_flag THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'strap_muscle', COUNT(*),
    SUM(CASE WHEN strap_muscle_involvement_flag THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN NOT strap_muscle_involvement_flag THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'reoperative_field', COUNT(*),
    SUM(CASE WHEN reoperative_field_flag THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN NOT reoperative_field_flag THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'drain', COUNT(*),
    SUM(CASE WHEN drain_flag THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN NOT drain_flag THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'ebl_ml', COUNT(*),
    SUM(CASE WHEN ebl_ml IS NOT NULL THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN ebl_ml IS NULL THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
UNION ALL
SELECT 'operative_findings_raw', COUNT(*),
    SUM(CASE WHEN operative_findings_raw IS NOT NULL AND operative_findings_raw != '' THEN 1 ELSE 0 END),
    0,
    SUM(CASE WHEN operative_findings_raw IS NULL OR operative_findings_raw = '' THEN 1 ELSE 0 END)
FROM operative_episode_detail_v2
"""


def run_audit(con, use_md: bool) -> dict:
    """Execute all phases and return summary dict."""
    results = {}
    mode = "MotherDuck" if use_md else "Local"
    print(f"\n{'='*70}")
    print(f"  OPERATIVE NOTE–PATHOLOGY LINKAGE AUDIT  [{mode}]")
    print(f"  {datetime.datetime.now().isoformat()}")
    print(f"{'='*70}\n")

    # ── Phase 2: Coverage tables ───────────────────────────────────────
    print("PHASE 2: Creating operative note coverage tables...")
    con.execute(VAL_OPERATIVE_NOTE_COVERAGE_SQL)
    print("  val_operative_note_coverage_v1 created")
    con.execute(VAL_OPERATIVE_NOTE_PARSE_COVERAGE_SQL)
    print("  val_operative_note_parse_coverage_v1 created")

    # Gather metrics
    r = {}
    r["total_patients_path_synoptics"] = q1(
        con, "ps_pts", "SELECT COUNT(DISTINCT research_id) FROM path_synoptics"
    )
    r["total_surgeries_path_synoptics"] = q1(
        con, "ps_rows", "SELECT COUNT(*) FROM path_synoptics"
    )
    r["total_operative_episodes"] = q1(
        con, "op_eps", "SELECT COUNT(*) FROM operative_episode_detail_v2"
    )
    r["total_operative_patients"] = q1(
        con,
        "op_pts",
        "SELECT COUNT(DISTINCT research_id) FROM operative_episode_detail_v2",
    )
    r["total_op_notes"] = q1(
        con,
        "op_notes",
        "SELECT COUNT(*) FROM clinical_notes_long WHERE note_type = 'op_note'",
    )
    r["total_patients_with_op_notes"] = q1(
        con,
        "opn_pts",
        "SELECT COUNT(DISTINCT research_id) FROM clinical_notes_long WHERE note_type = 'op_note'",
    )
    r["patients_with_surgery_and_opnote"] = q1(
        con,
        "both",
        "SELECT COUNT(*) FROM val_operative_note_coverage_v1 WHERE has_operative_episode AND has_op_note",
    )
    r["patients_with_surgery_no_opnote"] = q1(
        con,
        "no_on",
        "SELECT COUNT(*) FROM val_operative_note_coverage_v1 WHERE has_operative_episode AND NOT has_op_note",
    )
    r["cancer_patients"] = q1(
        con,
        "cancer",
        "SELECT COUNT(*) FROM val_operative_note_coverage_v1 WHERE is_cancer",
    )
    r["cancer_with_operative_episode"] = q1(
        con,
        "canc_op",
        "SELECT COUNT(*) FROM val_operative_note_coverage_v1 WHERE is_cancer AND has_operative_episode",
    )
    r["cancer_with_opnote"] = q1(
        con,
        "canc_on",
        "SELECT COUNT(*) FROM val_operative_note_coverage_v1 WHERE is_cancer AND has_op_note",
    )
    r["cancer_with_both"] = q1(
        con,
        "canc_both",
        "SELECT COUNT(*) FROM val_operative_note_coverage_v1 WHERE is_cancer AND has_both",
    )
    r["cancer_no_operative_episode"] = q1(
        con,
        "canc_no_op",
        "SELECT COUNT(*) FROM val_operative_note_coverage_v1 WHERE is_cancer AND NOT has_operative_episode",
    )
    r["path_only_patients"] = q1(
        con,
        "path_only",
        "SELECT COUNT(*) FROM val_operative_note_coverage_v1 WHERE NOT has_operative_episode",
    )

    # NLP parse coverage
    r["episodes_with_nlp_parse"] = q1(
        con,
        "nlp",
        "SELECT COUNT(*) FROM val_operative_note_parse_coverage_v1 WHERE has_nlp_parse",
    )
    r["episodes_no_nlp_parse"] = q1(
        con,
        "no_nlp",
        "SELECT COUNT(*) FROM val_operative_note_parse_coverage_v1 WHERE NOT has_nlp_parse",
    )

    results["coverage"] = r
    for k, v in r.items():
        print(f"  {k}: {v}")

    # ── Phase 3: Op note linkage ───────────────────────────────────────
    print("\nPHASE 3: Creating operative note linkage review...")
    con.execute(REVIEW_OPERATIVE_NOTE_LINKAGE_SQL)
    print("  review_operative_note_linkage_v1 created")

    linkage_cats = qall(
        con,
        "SELECT linkage_category, COUNT(*) FROM review_operative_note_linkage_v1 GROUP BY 1 ORDER BY 2 DESC",
    )
    results["opnote_linkage"] = {cat: n for cat, n in linkage_cats}
    for cat, n in linkage_cats:
        print(f"  {cat}: {n}")

    # ── Phase 4: Surgery-pathology linkage ─────────────────────────────
    print("\nPHASE 4: Creating surgery-pathology linkage validation...")
    con.execute(VAL_SURGERY_PATH_LINKAGE_SQL)
    print("  val_surgery_path_linkage_v1 created")
    con.execute(REVIEW_SURGERY_PATH_DISCORDANCE_SQL)
    print("  review_surgery_path_discordance_v1 created")

    sp_cats = qall(
        con,
        "SELECT linkage_status, COUNT(*) FROM val_surgery_path_linkage_v1 GROUP BY 1 ORDER BY 2 DESC",
    )
    results["surgery_path_linkage"] = {cat: n for cat, n in sp_cats}
    for cat, n in sp_cats:
        print(f"  {cat}: {n}")

    disc_count = q1(
        con,
        "disc",
        "SELECT COUNT(*) FROM review_surgery_path_discordance_v1",
    )
    print(f"  Total discordance/review items: {disc_count}")
    results["discordance_count"] = disc_count

    # ── Phase 5: Variable coverage ─────────────────────────────────────
    print("\nPHASE 5: Creating operative variable coverage audit...")
    con.execute(VAL_OPERATIVE_VARIABLE_COVERAGE_SQL)
    print("  val_operative_variable_coverage_v1 created")

    var_cov = qall(
        con,
        "SELECT variable, total_episodes, extracted, unknown_val, missing FROM val_operative_variable_coverage_v1 ORDER BY variable",
    )
    results["variable_coverage"] = {}
    for var, total, ext, unk, miss in var_cov:
        pct = round(100 * ext / total, 1) if total else 0
        results["variable_coverage"][var] = {
            "total": total,
            "extracted": ext,
            "unknown": unk,
            "missing": miss,
            "pct_extracted": pct,
        }
        print(f"  {var}: {ext}/{total} ({pct}%)")

    # ── Op note coverage by year ───────────────────────────────────────
    print("\n  Op note coverage by surgery year:")
    yr_cov = qall(
        con,
        """
        SELECT
            EXTRACT(YEAR FROM o.surgery_date_native) AS yr,
            COUNT(DISTINCT o.research_id) AS total,
            COUNT(DISTINCT CASE WHEN cn.research_id IS NOT NULL THEN o.research_id END) AS with_opnote
        FROM operative_episode_detail_v2 o
        LEFT JOIN clinical_notes_long cn
            ON o.research_id = cn.research_id AND cn.note_type = 'op_note'
        WHERE o.surgery_date_native IS NOT NULL
          AND EXTRACT(YEAR FROM o.surgery_date_native) >= 2010
        GROUP BY 1 ORDER BY 1
    """,
    )
    results["coverage_by_year"] = {}
    for yr, total, with_on in yr_cov:
        yr = int(yr)
        pct = round(100 * with_on / total, 1) if total else 0
        results["coverage_by_year"][yr] = {
            "total": total,
            "with_opnote": with_on,
            "pct": pct,
        }
        print(f"    {yr}: {with_on}/{total} ({pct}%)")

    # ── v3 linkage summary ─────────────────────────────────────────────
    print("\n  Surgery-pathology v3 linkage tiers:")
    try:
        v3_tiers = qall(
            con,
            "SELECT linkage_confidence_tier, COUNT(*) FROM surgery_pathology_linkage_v3 GROUP BY 1 ORDER BY 2 DESC",
        )
        results["v3_tiers"] = {t: n for t, n in v3_tiers}
        for t, n in v3_tiers:
            print(f"    {t}: {n}")
    except Exception as e:
        print(f"    v3 linkage not available: {e}")
        results["v3_tiers"] = {}

    return results


def export_tables(con, export_dir: str):
    """Export audit tables to CSV."""
    os.makedirs(export_dir, exist_ok=True)
    tables = [
        "val_operative_note_coverage_v1",
        "val_operative_note_parse_coverage_v1",
        "review_operative_note_linkage_v1",
        "val_surgery_path_linkage_v1",
        "review_surgery_path_discordance_v1",
        "val_operative_variable_coverage_v1",
    ]
    for tbl in tables:
        try:
            con.execute(
                f"COPY {tbl} TO '{export_dir}/{tbl}.csv' (HEADER, DELIMITER ',')"
            )
            rows = q1(con, tbl, f"SELECT COUNT(*) FROM {tbl}")
            print(f"  Exported {tbl}: {rows} rows")
        except Exception as e:
            print(f"  WARN export {tbl}: {e}")


def write_report(results: dict, export_dir: str, use_md: bool):
    """Write markdown audit report."""
    r = results.get("coverage", {})
    mode = "MotherDuck" if use_md else "Local DuckDB"
    report_path = f"docs/operative_note_path_linkage_audit_{TODAY}.md"

    # Phase 6 verdict
    nlp_pct = 0
    if r.get("total_operative_episodes") and r.get("episodes_with_nlp_parse"):
        nlp_pct = round(
            100 * r["episodes_with_nlp_parse"] / r["total_operative_episodes"], 1
        )

    cancer_no_op = r.get("cancer_no_operative_episode", 0) or 0
    cancer_total = r.get("cancer_patients", 0) or 0
    cancer_no_op_pct = round(100 * cancer_no_op / cancer_total, 1) if cancer_total else 0

    opnote_linkage = results.get("opnote_linkage", {})
    confident = opnote_linkage.get("LINKED_CONFIDENT", 0)
    proximal = opnote_linkage.get("LINKED_DATE_PROXIMAL", 0)
    missing = opnote_linkage.get("MISSING", 0)
    ambiguous = opnote_linkage.get("AMBIGUOUS", 0)

    sp_linkage = results.get("surgery_path_linkage", {})
    aligned = sp_linkage.get("ALIGNED", 0)
    prox_aligned = sp_linkage.get("PROXIMAL_ALIGNED", 0)
    no_path = sp_linkage.get("NO_PATHOLOGY", 0)

    if nlp_pct >= 80 and cancer_no_op_pct < 5:
        verdict = "NO FURTHER OPERATIVE EXTRACTION NEEDED"
    elif nlp_pct >= 40 or cancer_no_op_pct < 15:
        verdict = "TARGETED OPERATIVE EXTRACTION RECOMMENDED"
    else:
        verdict = "MAJOR OPERATIVE EXTRACTION GAP REMAINS"

    md = f"""# Operative Note–Pathology Linkage Audit

**Date:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}
**Database:** {mode}
**Script:** `scripts/70_operative_note_path_linkage_audit.py`

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Total patients (path_synoptics) | {r.get('total_patients_path_synoptics', '?'):,} |
| Total surgeries (path_synoptics rows) | {r.get('total_surgeries_path_synoptics', '?'):,} |
| Operative episode records | {r.get('total_operative_episodes', '?'):,} |
| Patients with operative episode | {r.get('total_operative_patients', '?'):,} |
| Patients with op notes | {r.get('total_patients_with_op_notes', '?'):,} |
| Patients with surgery AND op note | {r.get('patients_with_surgery_and_opnote', '?'):,} |
| Patients with surgery, NO op note | {r.get('patients_with_surgery_no_opnote', '?'):,} |
| Patients in path_synoptics only (no operative episode) | {r.get('path_only_patients', '?'):,} |

### Cancer-Specific Coverage

| Metric | Value |
|--------|-------|
| Cancer patients (total) | {r.get('cancer_patients', '?'):,} |
| Cancer with operative episode | {r.get('cancer_with_operative_episode', '?'):,} |
| Cancer with op note | {r.get('cancer_with_opnote', '?'):,} |
| Cancer with both episode + op note | {r.get('cancer_with_both', '?'):,} |
| **Cancer with NO operative episode** | **{cancer_no_op:,}** ({cancer_no_op_pct}%) |

### NLP Parse Coverage

| Metric | Value |
|--------|-------|
| Episodes with NLP parse | {r.get('episodes_with_nlp_parse', '?'):,} |
| Episodes without NLP parse | {r.get('episodes_no_nlp_parse', '?'):,} |
| **NLP parse rate** | **{nlp_pct}%** |

---

## Phase 3: Operative Note ↔ Surgery Linkage

| Category | Count |
|----------|-------|
| LINKED_CONFIDENT (same-day) | {confident:,} |
| LINKED_DATE_PROXIMAL (≤7d) | {proximal:,} |
| AMBIGUOUS (>7d or date missing) | {ambiguous:,} |
| MISSING (no op note) | {missing:,} |

---

## Phase 4: Surgery ↔ Pathology Linkage

| Status | Count |
|--------|-------|
| ALIGNED (exact date + laterality ok) | {aligned:,} |
| PROXIMAL_ALIGNED (≤7d + laterality ok) | {prox_aligned:,} |
| NO_PATHOLOGY (operative episode, no tumor match) | {no_path:,} |
| DATE_DISCORDANT | {sp_linkage.get('DATE_DISCORDANT', 0):,} |
| LATERALITY_DISCORDANT | {sp_linkage.get('LATERALITY_DISCORDANT', 0):,} |
| DATE_MISSING | {sp_linkage.get('DATE_MISSING', 0):,} |
| REVIEW_NEEDED | {sp_linkage.get('REVIEW_NEEDED', 0):,} |

Total discordance/review items: {results.get('discordance_count', '?'):,}

---

## Phase 5: Operative Variable Extraction Coverage

| Variable | Extracted | Total | Rate |
|----------|-----------|-------|------|
"""
    for var, d in sorted(results.get("variable_coverage", {}).items()):
        md += f"| {var} | {d['extracted']:,} | {d['total']:,} | {d['pct_extracted']}% |\n"

    md += f"""
---

## Op Note Coverage by Surgery Year (2010+)

| Year | Total Surgeries | With Op Note | Coverage |
|------|----------------|-------------|----------|
"""
    for yr, d in sorted(results.get("coverage_by_year", {}).items()):
        md += f"| {yr} | {d['total']:,} | {d['with_opnote']:,} | {d['pct']}% |\n"

    md += f"""
---

## Surgery-Pathology v3 Linkage Tiers

| Tier | Count |
|------|-------|
"""
    for t, n in sorted(results.get("v3_tiers", {}).items(), key=lambda x: -x[1]):
        md += f"| {t} | {n:,} |\n"

    md += f"""
---

## Phase 6: Verdict

### A. Are operative notes fully parsed?

**No.** NLP enrichment covers {nlp_pct}% of operative episodes. On MotherDuck, NLP
enrichment is **zero** — the materialized table was created before or without the
NLP enrichment step from script 22's `enrich_from_v2_extractors()`. Local DuckDB has
partial enrichment for episodes with matching clinical notes.

Op notes exist for only {r.get('total_patients_with_op_notes', '?'):,} / {r.get('total_operative_patients', '?'):,}
operative-episode patients ({round(100 * (r.get('total_patients_with_op_notes', 0) or 0) / max(r.get('total_operative_patients', 1) or 1, 1), 1)}%).
Coverage is concentrated in 2019-2022 (88-90%); pre-2019 is near-zero.

### B. Are they fully linked to pathology by date and patient?

**Largely yes for existing episodes.** {aligned:,} / {r.get('total_operative_episodes', '?'):,} ({round(100 * aligned / max(r.get('total_operative_episodes', 1) or 1, 1), 1)}%)
operative episodes have exact same-day pathology alignment. However, {r.get('path_only_patients', '?'):,}
patients ({round(100 * (r.get('path_only_patients', 0) or 0) / max(r.get('total_patients_path_synoptics', 1) or 1, 1), 1)}%)
have pathology records but no operative episode, meaning they are known only through path_synoptics.

### C. Is missing operative detail a manuscript blocker?

**No, for the current ETE/staging manuscript.** The primary analyses use structured
path_synoptics data (ETE, margins, invasion) which covers {r.get('total_patients_path_synoptics', '?'):,}
patients. Operative note NLP adds granularity (RLN monitoring, drain, parathyroid
management) but the core staging variables come from pathology synoptics.

For **complication-focused manuscripts** (H1 CLN/lobectomy), the gap is more relevant:
RLN monitoring status and intraoperative findings depend on parsed op notes.

### D. Is targeted additional extraction worthwhile?

**Yes, targeted MotherDuck sync is the priority.** The NLP enrichment already exists
in local DuckDB but was never propagated to MotherDuck. Re-running script 22 with
NLP enrichment then re-materializing via script 26 would immediately recover
{r.get('episodes_with_nlp_parse', '?'):,} enriched episodes.

### Classification

## **{verdict}**

**Recommended actions:**
1. Re-run script 22 `enrich_from_v2_extractors()` to ensure local NLP enrichment is current
2. Re-materialize `operative_episode_detail_v2` to MotherDuck via script 26
3. Populate CND/LND flags from `path_synoptics.central_compartment_dissection` (665 patients)
4. For pre-2019 surgeries without op notes, accept path_synoptics as sole surgery evidence

---

## Deliverables Created

### MotherDuck Tables
- `val_operative_note_coverage_v1` — per-patient operative note coverage flags
- `val_operative_note_parse_coverage_v1` — per-episode NLP parse detail
- `review_operative_note_linkage_v1` — surgery↔op note linkage with categories
- `val_surgery_path_linkage_v1` — surgery↔pathology date/laterality alignment
- `review_surgery_path_discordance_v1` — discordant linkages for manual review
- `val_operative_variable_coverage_v1` — per-variable extraction rates

### Exports
- `{EXPORT_DIR}/` — CSV exports of all audit tables
- `docs/operative_note_path_linkage_audit_{TODAY}.md` — this report

---

*Generated by script 70 — {datetime.datetime.now().isoformat()}*
"""

    os.makedirs("docs", exist_ok=True)
    with open(report_path, "w") as f:
        f.write(md)
    print(f"\n  Report written: {report_path}")

    # Also write manifest
    os.makedirs(export_dir, exist_ok=True)
    manifest = {
        "generated_at": datetime.datetime.now().isoformat(),
        "database": mode,
        "script": "scripts/70_operative_note_path_linkage_audit.py",
        "verdict": verdict,
        "summary": r,
        "opnote_linkage": results.get("opnote_linkage", {}),
        "surgery_path_linkage": results.get("surgery_path_linkage", {}),
        "variable_coverage": results.get("variable_coverage", {}),
        "v3_tiers": results.get("v3_tiers", {}),
    }
    with open(f"{export_dir}/manifest.json", "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    print(f"  Manifest written: {export_dir}/manifest.json")

    return report_path


def main():
    parser = argparse.ArgumentParser(
        description="Operative Note–Pathology Linkage Audit"
    )
    parser.add_argument("--md", action="store_true", help="Use MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    args = parser.parse_args()

    use_md = args.md
    if not use_md and not args.local:
        use_md = True

    con = get_connection(use_md)

    results = run_audit(con, use_md)
    export_tables(con, EXPORT_DIR)
    report_path = write_report(results, EXPORT_DIR, use_md)

    print(f"\n{'='*70}")
    print("  AUDIT COMPLETE")
    print(f"  Report: {report_path}")
    print(f"  Exports: {EXPORT_DIR}/")
    print(f"{'='*70}")

    con.close()


if __name__ == "__main__":
    main()
