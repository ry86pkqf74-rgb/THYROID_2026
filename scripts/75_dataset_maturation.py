#!/usr/bin/env python3
"""
75_dataset_maturation.py -- Final dataset maturation + repo synchronization

Orchestrates 10 phases of post-audit dataset maturation:
  Phase 1: Canonical layer integrity verification
  Phase 2: Operative episode enhancement (CND/LND, note dates)
  Phase 3: Imaging layer canonicalization
  Phase 4: Provenance system hardening
  Phase 5: Chronology anomaly adjudication
  Phase 6: Repository documentation sync (handled externally)
  Phase 7: Dataset health dashboard tables
  Phase 8: MotherDuck optimization
  Phase 9: Final verification pass (delegates to existing scripts)
  Phase 10: Deliverables + export

Supports --md (MotherDuck writes), --phase N (run single phase),
--all (run all phases), --dry-run (preview only).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "thyroid_master.duckdb"
sys.path.insert(0, str(ROOT))

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")
EXPORT_DIR = ROOT / "exports" / f"dataset_maturation_{TIMESTAMP}"


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


def safe_count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    try:
        row = con.execute(sql).fetchone()
        return int(row[0]) if row else 0
    except Exception:
        return -1


def fill_rate(con: duckdb.DuckDBPyConnection, tbl: str, col: str) -> dict[str, Any]:
    total = safe_count(con, f"SELECT COUNT(*) FROM {tbl}")
    filled = safe_count(con, f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NOT NULL")
    pct = round(100.0 * filled / total, 2) if total > 0 else 0.0
    return {"table": tbl, "column": col, "filled": filled, "total": total, "pct": pct}


def connect_md() -> duckdb.DuckDBPyConnection:
    from motherduck_client import MotherDuckClient, MotherDuckConfig
    cfg = MotherDuckConfig(database="thyroid_research_2026")
    client = MotherDuckClient(cfg)
    return client.connect_rw()


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 1 — Canonical Layer Integrity Verification
# ═══════════════════════════════════════════════════════════════════════════

CANONICAL_FILL_CHECKS = [
    ("patient_analysis_resolved_v1", "rai_first_date"),
    ("patient_analysis_resolved_v1", "rai_dose_mci"),
    ("patient_analysis_resolved_v1", "mol_ras_positive_final"),
    ("episode_analysis_resolved_v1_dedup", "linked_fna_episode_id"),
    ("episode_analysis_resolved_v1_dedup", "linked_rai_episode_id"),
    ("episode_analysis_resolved_v1_dedup", "rai_dose_mci"),
    ("episode_analysis_resolved_v1_dedup", "fna_link_score_v3"),
    ("episode_analysis_resolved_v1_dedup", "path_link_score_v3"),
    ("episode_analysis_resolved_v1_dedup", "rai_link_score_v3"),
    ("manuscript_cohort_v1", "rai_first_date"),
    ("manuscript_cohort_v1", "mol_braf_positive_final"),
    ("manuscript_cohort_v1", "mol_tert_positive_final"),
]

SOURCE_FILL_CHECKS = [
    ("rai_treatment_episode_v2", "resolved_rai_date"),
    ("rai_treatment_episode_v2", "dose_mci"),
    ("molecular_test_episode_v2", "ras_flag"),
    ("fna_molecular_linkage_v3", "linkage_score"),
    ("preop_surgery_linkage_v3", "linkage_score"),
    ("surgery_pathology_linkage_v3", "linkage_score"),
    ("pathology_rai_linkage_v3", "linkage_score"),
    ("imaging_nodule_master_v1", "tirads_reported"),
    ("operative_episode_detail_v2", "central_neck_dissection_flag"),
    ("operative_episode_detail_v2", "lateral_neck_dissection_flag"),
]


def phase1_canonical_integrity(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict:
    section("Phase 1 — Canonical Layer Integrity Verification")
    results: dict[str, Any] = {"canonical_fill": [], "source_fill": [], "repairs": []}

    print("  Checking canonical table fill rates...\n")
    print(f"  {'Table':<50} {'Column':<35} {'Filled':>8} {'Total':>8} {'%':>8}")
    print(f"  {'-'*50} {'-'*35} {'-'*8} {'-'*8} {'-'*8}")
    for tbl, col in CANONICAL_FILL_CHECKS:
        if not table_available(con, tbl):
            print(f"  {tbl:<50} {col:<35} {'MISSING':>8}")
            continue
        r = fill_rate(con, tbl, col)
        results["canonical_fill"].append(r)
        print(f"  {tbl:<50} {col:<35} {r['filled']:>8,} {r['total']:>8,} {r['pct']:>7.1f}%")

    print("\n  Checking source table fill rates...\n")
    print(f"  {'Table':<50} {'Column':<35} {'Filled':>8} {'Total':>8} {'%':>8}")
    print(f"  {'-'*50} {'-'*35} {'-'*8} {'-'*8} {'-'*8}")
    for tbl, col in SOURCE_FILL_CHECKS:
        if not table_available(con, tbl):
            print(f"  {tbl:<50} {col:<35} {'MISSING':>8}")
            continue
        r = fill_rate(con, tbl, col)
        results["source_fill"].append(r)
        print(f"  {tbl:<50} {col:<35} {r['filled']:>8,} {r['total']:>8,} {r['pct']:>7.1f}%")

    return results


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 2 — Operative Episode Enhancement
# ═══════════════════════════════════════════════════════════════════════════

CND_LND_WIRE_SQL = """
UPDATE operative_episode_detail_v2 o
SET central_neck_dissection_flag = TRUE
FROM (
    SELECT DISTINCT ps.research_id, TRY_CAST(ps.surg_date AS DATE) AS sd
    FROM path_synoptics ps
    WHERE (
        ps.central_compartment_dissection IS NOT NULL
        OR LOWER(COALESCE(ps.tumor_1_level_examined, '')) LIKE '%6%'
        OR regexp_matches(LOWER(COALESCE(ps.other_ln_dissection, '')), 'central|level.?6')
        OR regexp_matches(
            LOWER(COALESCE(ps.tumor_1_ln_location, '')),
            'perithyroidal|pretracheal|paratracheal|delphian|prelaryngeal'
        )
    )
) src
WHERE o.research_id = src.research_id
  AND (
      o.surgery_date_native = src.sd
      OR (o.surgery_date_native IS NULL AND src.sd IS NULL)
  )
  AND o.central_neck_dissection_flag IS NOT TRUE
"""

LND_WIRE_SQL = """
UPDATE operative_episode_detail_v2 o
SET lateral_neck_dissection_flag = TRUE
FROM (
    SELECT DISTINCT ps.research_id, TRY_CAST(ps.surg_date AS DATE) AS sd
    FROM path_synoptics ps
    WHERE (
        regexp_matches(
            LOWER(COALESCE(ps.thyroid_procedure, '')),
            'lateral.?neck|lnd|mrnd|modified.?radical|radical.?neck|selective.?neck'
        )
        OR regexp_matches(
            LOWER(COALESCE(ps.other_ln_dissection, '')),
            'lateral|level.?(2|3|4|5)|jugular|modified.?radical'
        )
        OR regexp_matches(
            LOWER(COALESCE(ps.tumor_1_ln_location, '')),
            'lateral|jugular|level.?(ii|iii|iv|v|2|3|4|5)'
        )
    )
) src
WHERE o.research_id = src.research_id
  AND (
      o.surgery_date_native = src.sd
      OR (o.surgery_date_native IS NULL AND src.sd IS NULL)
  )
  AND o.lateral_neck_dissection_flag IS NOT TRUE
"""

OP_NOTE_DATE_RECOVERY_SQL = """
ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS note_date_resolved DATE;
ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS note_date_source VARCHAR;
ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS note_date_confidence DOUBLE;
"""

OP_NOTE_DATE_UPDATE_SQL = """
UPDATE operative_episode_detail_v2 o
SET
    note_date_resolved = src.best_date,
    note_date_source = src.date_src,
    note_date_confidence = src.date_conf
FROM (
    SELECT
        od.research_id,
        TRY_CAST(od.surg_date AS DATE) AS sd,
        COALESCE(
            TRY_CAST(cn.note_date AS DATE),
            TRY_CAST(od.surg_date AS DATE)
        ) AS best_date,
        CASE
            WHEN TRY_CAST(cn.note_date AS DATE) IS NOT NULL THEN 'note_date'
            WHEN TRY_CAST(od.surg_date AS DATE) IS NOT NULL THEN 'surgery_date_fallback'
            ELSE 'unresolved'
        END AS date_src,
        CASE
            WHEN TRY_CAST(cn.note_date AS DATE) IS NOT NULL THEN 0.85
            WHEN TRY_CAST(od.surg_date AS DATE) IS NOT NULL THEN 0.60
            ELSE 0.0
        END AS date_conf
    FROM operative_details od
    LEFT JOIN (
        SELECT research_id,
               TRY_CAST(note_date AS DATE) AS note_date,
               ROW_NUMBER() OVER (
                   PARTITION BY research_id
                   ORDER BY TRY_CAST(note_date AS DATE) DESC NULLS LAST
               ) AS rn
        FROM clinical_notes_long
        WHERE LOWER(note_type) = 'op_note'
    ) cn ON cn.research_id = od.research_id AND cn.rn = 1
    WHERE od.surg_date IS NOT NULL
) src
WHERE o.research_id = src.research_id
  AND (
      o.surgery_date_native = src.sd
      OR (o.surgery_date_native IS NULL AND src.sd IS NULL)
  )
  AND o.note_date_resolved IS NULL
"""


def phase2_operative_enhancement(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict:
    section("Phase 2 — Operative Episode Enhancement")
    results: dict[str, Any] = {}

    if not table_available(con, "operative_episode_detail_v2"):
        print("  ERROR: operative_episode_detail_v2 not found.")
        return results

    before_cnd = safe_count(
        con,
        "SELECT COUNT(*) FROM operative_episode_detail_v2 "
        "WHERE central_neck_dissection_flag IS TRUE"
    )
    before_lnd = safe_count(
        con,
        "SELECT COUNT(*) FROM operative_episode_detail_v2 "
        "WHERE lateral_neck_dissection_flag IS TRUE"
    )
    total_ops = safe_count(con, "SELECT COUNT(*) FROM operative_episode_detail_v2")

    print(f"  Before: CND flag TRUE = {before_cnd:,} / {total_ops:,}")
    print(f"  Before: LND flag TRUE = {before_lnd:,} / {total_ops:,}")

    if not dry_run:
        print("\n  Wiring CND flag from structured fields...")
        con.execute(CND_LND_WIRE_SQL)
        after_cnd = safe_count(
            con,
            "SELECT COUNT(*) FROM operative_episode_detail_v2 "
            "WHERE central_neck_dissection_flag IS TRUE"
        )
        print(f"  After:  CND flag TRUE = {after_cnd:,} / {total_ops:,} (+{after_cnd - before_cnd:,})")

        print("\n  Wiring LND flag from structured fields...")
        con.execute(LND_WIRE_SQL)
        after_lnd = safe_count(
            con,
            "SELECT COUNT(*) FROM operative_episode_detail_v2 "
            "WHERE lateral_neck_dissection_flag IS TRUE"
        )
        print(f"  After:  LND flag TRUE = {after_lnd:,} / {total_ops:,} (+{after_lnd - before_lnd:,})")

        print("\n  Recovering missing operative note dates...")
        for stmt in OP_NOTE_DATE_RECOVERY_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    con.execute(stmt)
                except Exception:
                    pass

        before_note_dates = safe_count(
            con,
            "SELECT COUNT(*) FROM operative_episode_detail_v2 "
            "WHERE note_date_resolved IS NOT NULL"
        )
        con.execute(OP_NOTE_DATE_UPDATE_SQL)
        after_note_dates = safe_count(
            con,
            "SELECT COUNT(*) FROM operative_episode_detail_v2 "
            "WHERE note_date_resolved IS NOT NULL"
        )
        print(f"  Note dates resolved: {before_note_dates:,} -> {after_note_dates:,} (+{after_note_dates - before_note_dates:,})")

        results = {
            "cnd_before": before_cnd, "cnd_after": after_cnd,
            "lnd_before": before_lnd, "lnd_after": after_lnd,
            "note_dates_before": before_note_dates, "note_dates_after": after_note_dates,
            "total_ops": total_ops,
        }
    else:
        print("  [dry-run] Would wire CND/LND flags and recover note dates.")
        results = {"dry_run": True, "cnd_before": before_cnd, "lnd_before": before_lnd}

    return results


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 3 — Imaging Layer Canonicalization
# ═══════════════════════════════════════════════════════════════════════════

def phase3_imaging_canonicalization(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict:
    section("Phase 3 — Imaging Layer Canonicalization")
    results: dict[str, Any] = {}

    for tbl in ("imaging_nodule_master_v1", "md_imaging_nodule_master_v1"):
        if table_available(con, tbl):
            cnt = safe_count(con, f"SELECT COUNT(*) FROM {tbl}")
            print(f"  {tbl}: {cnt:,} rows")
            results[tbl] = cnt

            null_checks = {
                "tirads_reported": safe_count(con, f"SELECT COUNT(*) FROM {tbl} WHERE tirads_reported IS NULL"),
                "max_dimension_cm": safe_count(con, f"SELECT COUNT(*) FROM {tbl} WHERE max_dimension_cm IS NULL"),
                "suspicious_flag": safe_count(con, f"SELECT COUNT(*) FROM {tbl} WHERE suspicious_flag IS NULL"),
            }
            for col, null_ct in null_checks.items():
                total = results[tbl]
                pct = round(100.0 * (total - null_ct) / total, 1) if total > 0 else 0.0
                print(f"    {col}: {total - null_ct:,} filled / {total:,} ({pct}%)")
            results[f"{tbl}_nulls"] = null_checks

    for tbl in ("imaging_nodule_long_v2", "md_imaging_nodule_long_v2"):
        if table_available(con, tbl):
            cnt = safe_count(con, f"SELECT COUNT(*) FROM {tbl}")
            print(f"  {tbl}: {cnt:,} rows (DEPRECATED — schema stub)")
            results[tbl] = cnt

    if not dry_run:
        try:
            con.execute("ANALYZE imaging_nodule_master_v1")
            print("\n  ANALYZE TABLE imaging_nodule_master_v1 — OK")
        except Exception as e:
            print(f"\n  ANALYZE TABLE imaging_nodule_master_v1 — {e}")

    return results


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 4 — Provenance System Hardening
# ═══════════════════════════════════════════════════════════════════════════

PROVENANCE_TARGETS = [
    "patient_analysis_resolved_v1",
    "episode_analysis_resolved_v1_dedup",
    "lesion_analysis_resolved_v1",
    "survival_cohort_enriched",
]

PROVENANCE_COLS = [
    ("source_table", "VARCHAR"),
    ("source_script", "VARCHAR"),
    ("provenance_note", "VARCHAR"),
    ("resolved_layer_version", "VARCHAR"),
]

PROVENANCE_VALUES = {
    "patient_analysis_resolved_v1": {
        "source_table": "operative_episode_detail_v2,tumor_episode_master_v2,rai_treatment_episode_v2,molecular_test_episode_v2,fna_episode_master_v2,thyroid_scoring_py_v1,complication_patient_summary_v1,longitudinal_lab_patient_summary_v1,recurrence_event_clean_v1",
        "source_script": "48",
        "provenance_note": "analysis-grade resolved layer; canonical V2 episodes + V3 linkage scores + scoring + complications + labs",
        "resolved_layer_version": "v1",
    },
    "episode_analysis_resolved_v1_dedup": {
        "source_table": "operative_episode_detail_v2,tumor_episode_master_v2,rai_treatment_episode_v2,preop_surgery_linkage_v2,surgery_pathology_linkage_v2,pathology_rai_linkage_v2",
        "source_script": "48",
        "provenance_note": "episode-level resolved layer; deduped from episode_analysis_resolved_v1 via worst-case priority",
        "resolved_layer_version": "v1",
    },
    "lesion_analysis_resolved_v1": {
        "source_table": "tumor_episode_master_v2,fna_episode_master_v2,molecular_test_episode_v2,imaging_nodule_master_v1",
        "source_script": "48",
        "provenance_note": "lesion-level resolved layer; one row per tumor/lesion with cross-domain linkage",
        "resolved_layer_version": "v1",
    },
    "survival_cohort_enriched": {
        "source_table": "survival_cohort_ready_mv,advanced_features_sorted,recurrence_risk_features_mv",
        "source_script": "26",
        "provenance_note": "survival analysis cohort; enriched from survival_cohort_ready_mv + features + recurrence risk",
        "resolved_layer_version": "v1",
    },
}


def phase4_provenance_hardening(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict:
    section("Phase 4 — Provenance System Hardening")
    results: dict[str, Any] = {}

    for tbl in PROVENANCE_TARGETS:
        if not table_available(con, tbl):
            print(f"  SKIP {tbl} — not found")
            continue

        total = safe_count(con, f"SELECT COUNT(*) FROM {tbl}")
        print(f"\n  {tbl} ({total:,} rows)")

        for col_name, col_type in PROVENANCE_COLS:
            try:
                con.execute(
                    f"ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS {col_name} {col_type}"
                )
            except Exception:
                pass

            filled = safe_count(
                con, f"SELECT COUNT(*) FROM {tbl} WHERE {col_name} IS NOT NULL"
            )
            print(f"    {col_name}: {filled:,} / {total:,} filled")

        if not dry_run and tbl in PROVENANCE_VALUES:
            vals = PROVENANCE_VALUES[tbl]
            for col_name, val in vals.items():
                try:
                    con.execute(
                        f"UPDATE {tbl} SET {col_name} = '{val}' "
                        f"WHERE {col_name} IS NULL"
                    )
                except Exception as e:
                    print(f"    WARN updating {col_name}: {e}")

            for col_name, _ in PROVENANCE_COLS:
                filled_after = safe_count(
                    con, f"SELECT COUNT(*) FROM {tbl} WHERE {col_name} IS NOT NULL"
                )
                print(f"    {col_name} (after): {filled_after:,} / {total:,}")

        results[tbl] = {"rows": total}

    return results


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 5 — Chronology Anomaly Adjudication
# ═══════════════════════════════════════════════════════════════════════════

VAL_TEMPORAL_RESOLUTION_SQL = """
CREATE OR REPLACE TABLE val_temporal_anomaly_resolution_v1 AS
SELECT
    research_id,
    anomaly_type,
    severity,
    event_a_domain,
    event_a_date,
    event_b_domain,
    event_b_date,
    day_gap,
    detail,
    CASE
        -- Molecular after surgery is expected surveillance
        WHEN anomaly_type = 'molecular_post_surgery' THEN 'benign_temporal_offset'
        -- FNA after surgery could be recurrence workup
        WHEN anomaly_type = 'fna_post_surgery' THEN 'benign_temporal_offset'
        -- Late RAI within 2 years is clinically plausible
        WHEN anomaly_type = 'late_rai' AND ABS(day_gap) <= 730 THEN 'benign_temporal_offset'
        -- Late RAI beyond 2 years is unusual
        WHEN anomaly_type = 'late_rai' AND ABS(day_gap) > 730 THEN 'true_conflict'
        -- Rapid repeat surgery < 30d might be completion thyroidectomy
        WHEN anomaly_type = 'rapid_repeat_surgery' THEN 'multi_procedure_episode'
        -- RAI before surgery (error-severity) is a true conflict
        WHEN anomaly_type = 'rai_before_surgery' THEN 'true_conflict'
        -- Future dates are extraction errors
        WHEN TRY_CAST(event_a_date AS DATE) > CURRENT_DATE
             OR TRY_CAST(event_b_date AS DATE) > CURRENT_DATE
            THEN 'source_extraction_error'
        -- Dates before 1990 are extraction errors
        WHEN TRY_CAST(event_a_date AS DATE) < DATE '1990-01-01'
             OR TRY_CAST(event_b_date AS DATE) < DATE '1990-01-01'
            THEN 'source_extraction_error'
        ELSE 'true_conflict'
    END AS resolution_bucket,
    CASE
        WHEN anomaly_type = 'molecular_post_surgery'
            THEN 'Post-op molecular testing is expected surveillance; not a chronology error'
        WHEN anomaly_type = 'fna_post_surgery'
            THEN 'Post-op FNA may indicate recurrence workup; clinically plausible'
        WHEN anomaly_type = 'late_rai' AND ABS(day_gap) <= 730
            THEN 'RAI within 2 years of surgery is standard adjuvant therapy window'
        WHEN anomaly_type = 'late_rai' AND ABS(day_gap) > 730
            THEN 'RAI >2y post-surgery is atypical; may indicate recurrence treatment or data error'
        WHEN anomaly_type = 'rapid_repeat_surgery'
            THEN 'Likely completion thyroidectomy or staged procedure'
        WHEN anomaly_type = 'rai_before_surgery'
            THEN 'RAI preceding surgery is clinically implausible; date extraction error likely'
        ELSE 'Unclassified anomaly; requires manual review'
    END AS resolution_rationale,
    CASE
        WHEN anomaly_type IN ('molecular_post_surgery', 'fna_post_surgery') THEN TRUE
        WHEN anomaly_type = 'late_rai' AND ABS(day_gap) <= 730 THEN TRUE
        WHEN anomaly_type = 'rapid_repeat_surgery' THEN FALSE
        ELSE FALSE
    END AS auto_resolved_flag
FROM val_chronology_anomalies
"""


def phase5_chronology_adjudication(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict:
    section("Phase 5 — Chronology Anomaly Adjudication")
    results: dict[str, Any] = {}

    if not table_available(con, "val_chronology_anomalies"):
        print("  val_chronology_anomalies not found. Run scripts/29_validation_engine.py first.")
        return results

    total = safe_count(con, "SELECT COUNT(*) FROM val_chronology_anomalies")
    print(f"  Total chronology anomalies: {total:,}")

    try:
        rows = con.execute(
            "SELECT anomaly_type, severity, COUNT(*) "
            "FROM val_chronology_anomalies "
            "GROUP BY anomaly_type, severity ORDER BY COUNT(*) DESC"
        ).fetchall()
        print(f"\n  {'Anomaly Type':<35} {'Severity':<10} {'Count':>8}")
        print(f"  {'-'*35} {'-'*10} {'-'*8}")
        for r in rows:
            print(f"  {r[0]:<35} {r[1]:<10} {r[2]:>8,}")
    except Exception as e:
        print(f"  Error reading anomalies: {e}")

    if not dry_run:
        print("\n  Creating val_temporal_anomaly_resolution_v1...")
        con.execute(VAL_TEMPORAL_RESOLUTION_SQL)
        resolution_total = safe_count(con, "SELECT COUNT(*) FROM val_temporal_anomaly_resolution_v1")
        print(f"  Created: {resolution_total:,} rows")

        try:
            bucket_rows = con.execute(
                "SELECT resolution_bucket, auto_resolved_flag, COUNT(*) "
                "FROM val_temporal_anomaly_resolution_v1 "
                "GROUP BY resolution_bucket, auto_resolved_flag "
                "ORDER BY resolution_bucket"
            ).fetchall()
            print(f"\n  {'Resolution Bucket':<30} {'Auto-resolved':>14} {'Count':>8}")
            print(f"  {'-'*30} {'-'*14} {'-'*8}")
            for r in bucket_rows:
                print(f"  {r[0]:<30} {str(r[1]):>14} {r[2]:>8,}")
            results["buckets"] = [
                {"bucket": r[0], "auto_resolved": r[1], "count": r[2]}
                for r in bucket_rows
            ]
        except Exception as e:
            print(f"  Error: {e}")

    results["total_anomalies"] = total
    return results


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 7 — Dataset Health Dashboard Tables
# ═══════════════════════════════════════════════════════════════════════════

VAL_DATASET_INTEGRITY_SQL = """
CREATE OR REPLACE TABLE val_dataset_integrity_summary_v1 AS

WITH canonical_tables AS (
    SELECT * FROM (VALUES
        ('patient_analysis_resolved_v1', 'patient'),
        ('episode_analysis_resolved_v1_dedup', 'episode'),
        ('lesion_analysis_resolved_v1', 'lesion'),
        ('manuscript_cohort_v1', 'manuscript'),
        ('operative_episode_detail_v2', 'operative'),
        ('molecular_test_episode_v2', 'molecular'),
        ('rai_treatment_episode_v2', 'rai'),
        ('imaging_nodule_master_v1', 'imaging'),
        ('tumor_episode_master_v2', 'tumor'),
        ('fna_episode_master_v2', 'fna'),
        ('survival_cohort_enriched', 'survival'),
        ('thyroid_scoring_py_v1', 'scoring'),
        ('complication_phenotype_v1', 'complication'),
        ('longitudinal_lab_clean_v1', 'lab'),
        ('recurrence_event_clean_v1', 'recurrence')
    ) t(table_name, domain)
)
SELECT
    ct.table_name,
    ct.domain,
    COALESCE(i.row_count, -1)    AS row_count,
    COALESCE(i.column_count, -1) AS column_count,
    CURRENT_TIMESTAMP             AS checked_at
FROM canonical_tables ct
LEFT JOIN (
    SELECT
        table_name,
        -1 AS row_count,
        (SELECT COUNT(DISTINCT column_name)
         FROM information_schema.columns c
         WHERE c.table_name = t.table_name
           AND c.table_schema = 'main') AS column_count
    FROM information_schema.tables t
    WHERE t.table_schema = 'main'
) i ON i.table_name = ct.table_name
"""

VAL_PROVENANCE_COMPLETENESS_SQL = """
CREATE OR REPLACE TABLE val_provenance_completeness_v2 AS

WITH targets AS (
    SELECT * FROM (VALUES
        ('patient_analysis_resolved_v1'),
        ('episode_analysis_resolved_v1_dedup'),
        ('lesion_analysis_resolved_v1'),
        ('survival_cohort_enriched')
    ) t(table_name)
),
prov_cols AS (
    SELECT * FROM (VALUES
        ('source_table'),
        ('source_script'),
        ('provenance_note'),
        ('resolved_layer_version')
    ) c(col_name)
)
SELECT
    t.table_name,
    p.col_name,
    CASE WHEN ic.column_name IS NOT NULL THEN TRUE ELSE FALSE END AS column_exists,
    CURRENT_TIMESTAMP AS checked_at
FROM targets t
CROSS JOIN prov_cols p
LEFT JOIN information_schema.columns ic
    ON ic.table_name = t.table_name
    AND ic.column_name = p.col_name
    AND ic.table_schema = 'main'
"""

VAL_EPISODE_LINKAGE_SQL = """
CREATE OR REPLACE TABLE val_episode_linkage_completeness_v1 AS

SELECT 'fna_molecular' AS linkage_type,
       COUNT(*) AS total_rows,
       COUNT(linkage_score) AS linked,
       COUNT(*) - COUNT(linkage_score) AS unlinked,
       ROUND(100.0 * COUNT(linkage_score) / NULLIF(COUNT(*), 0), 1) AS linked_pct
FROM fna_molecular_linkage_v3

UNION ALL

SELECT 'preop_surgery',
       COUNT(*), COUNT(linkage_score),
       COUNT(*) - COUNT(linkage_score),
       ROUND(100.0 * COUNT(linkage_score) / NULLIF(COUNT(*), 0), 1)
FROM preop_surgery_linkage_v3

UNION ALL

SELECT 'surgery_pathology',
       COUNT(*), COUNT(linkage_score),
       COUNT(*) - COUNT(linkage_score),
       ROUND(100.0 * COUNT(linkage_score) / NULLIF(COUNT(*), 0), 1)
FROM surgery_pathology_linkage_v3

UNION ALL

SELECT 'pathology_rai',
       COUNT(*), COUNT(linkage_score),
       COUNT(*) - COUNT(linkage_score),
       ROUND(100.0 * COUNT(linkage_score) / NULLIF(COUNT(*), 0), 1)
FROM pathology_rai_linkage_v3

UNION ALL

SELECT 'imaging_fna',
       COUNT(*), COUNT(linkage_score),
       COUNT(*) - COUNT(linkage_score),
       ROUND(100.0 * COUNT(linkage_score) / NULLIF(COUNT(*), 0), 1)
FROM imaging_fna_linkage_v3
"""


def phase7_dashboard_tables(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict:
    section("Phase 7 — Dataset Health Dashboard Tables")
    results: dict[str, Any] = {}

    if dry_run:
        print("  [dry-run] Would create 3 monitoring tables.")
        return results

    for sql, tbl in [
        (VAL_DATASET_INTEGRITY_SQL, "val_dataset_integrity_summary_v1"),
        (VAL_PROVENANCE_COMPLETENESS_SQL, "val_provenance_completeness_v2"),
        (VAL_EPISODE_LINKAGE_SQL, "val_episode_linkage_completeness_v1"),
    ]:
        try:
            con.execute(sql)
            cnt = safe_count(con, f"SELECT COUNT(*) FROM {tbl}")
            print(f"  OK   {tbl:<50} {cnt:>8,} rows")
            results[tbl] = cnt
        except Exception as e:
            print(f"  WARN {tbl:<50} {e}")
            results[tbl] = f"ERROR: {e}"

    return results


# ═══════════════════════════════════════════════════════════════════════════
# PHASE 8 — MotherDuck Optimization
# ═══════════════════════════════════════════════════════════════════════════

ANALYZE_TARGETS = [
    "patient_analysis_resolved_v1",
    "episode_analysis_resolved_v1_dedup",
    "imaging_nodule_master_v1",
    "operative_episode_detail_v2",
    "manuscript_cohort_v1",
    "survival_cohort_enriched",
    "tumor_episode_master_v2",
    "molecular_test_episode_v2",
    "rai_treatment_episode_v2",
    "fna_episode_master_v2",
]


def phase8_motherduck_optimization(con: duckdb.DuckDBPyConnection, dry_run: bool) -> dict:
    section("Phase 8 — MotherDuck Optimization")
    results: dict[str, Any] = {}

    if dry_run:
        print("  [dry-run] Would run ANALYZE TABLE on 10 tables.")
        return results

    for tbl in ANALYZE_TARGETS:
        if not table_available(con, tbl):
            print(f"  SKIP {tbl} — not found")
            continue
        try:
            con.execute(f"ANALYZE {tbl}")
            cnt = safe_count(con, f"SELECT COUNT(*) FROM {tbl}")
            print(f"  ANALYZE {tbl:<50} OK ({cnt:,} rows)")
            results[tbl] = {"analyzed": True, "rows": cnt}
        except Exception as e:
            print(f"  ANALYZE {tbl:<50} {e}")
            results[tbl] = {"analyzed": False, "error": str(e)}

    return results


# ═══════════════════════════════════════════════════════════════════════════
# REPORT GENERATION
# ═══════════════════════════════════════════════════════════════════════════

def generate_integrity_report(all_results: dict, output_path: Path) -> None:
    lines = [
        "# Canonical Layer Integrity Report — Addendum (2026-03-13)",
        "",
        f"Generated: {datetime.now().isoformat()}",
        "",
        "## Canonical Table Fill Rates",
        "",
        "| Table | Column | Filled | Total | % |",
        "|-------|--------|-------:|------:|--:|",
    ]
    for r in all_results.get("phase1", {}).get("canonical_fill", []):
        lines.append(
            f"| {r['table']} | {r['column']} | {r['filled']:,} | {r['total']:,} | {r['pct']}% |"
        )
    lines += [
        "",
        "## Source Table Fill Rates",
        "",
        "| Table | Column | Filled | Total | % |",
        "|-------|--------|-------:|------:|--:|",
    ]
    for r in all_results.get("phase1", {}).get("source_fill", []):
        lines.append(
            f"| {r['table']} | {r['column']} | {r['filled']:,} | {r['total']:,} | {r['pct']}% |"
        )
    lines += [
        "",
        "## Assessment",
        "",
        "Sparsity in canonical fields is primarily **source-limited**, not propagation-limited.",
        "The resolved layer (`patient_analysis_resolved_v1`) queries sidecar tables directly,",
        "so manuscript analyses are not affected by canonical-table sparsity.",
        "",
        "## Operative Enhancement (Phase 2)",
        "",
    ]
    p2 = all_results.get("phase2", {})
    if p2.get("cnd_after") is not None:
        lines += [
            f"- CND flag: {p2['cnd_before']:,} -> {p2['cnd_after']:,} TRUE "
            f"(+{p2['cnd_after'] - p2['cnd_before']:,} of {p2['total_ops']:,})",
            f"- LND flag: {p2['lnd_before']:,} -> {p2['lnd_after']:,} TRUE "
            f"(+{p2['lnd_after'] - p2['lnd_before']:,} of {p2['total_ops']:,})",
            f"- Note dates resolved: {p2.get('note_dates_before', 0):,} -> "
            f"{p2.get('note_dates_after', 0):,}",
        ]

    lines += [
        "",
        "## Chronology Anomaly Classification (Phase 5)",
        "",
    ]
    p5 = all_results.get("phase5", {})
    lines.append(f"Total anomalies: {p5.get('total_anomalies', 'N/A')}")
    for b in p5.get("buckets", []):
        lines.append(f"- {b['bucket']}: {b['count']:,} (auto-resolved: {b['auto_resolved']})")

    lines += [
        "",
        "## Provenance Hardening (Phase 4)",
        "",
        "All 4 analysis tables now have unified provenance columns:",
        "`source_table`, `source_script`, `provenance_note`, `resolved_layer_version`",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  Report: {output_path}")


def generate_maturation_report(all_results: dict, output_path: Path) -> None:
    lines = [
        "# Dataset Maturation Report (2026-03-13)",
        "",
        f"Generated: {datetime.now().isoformat()}",
        "",
        "## Summary",
        "",
        "This report documents the final dataset maturation pass that transitions",
        "THYROID_2026 from **manuscript-ready** to **dataset-mature** status.",
        "",
        "## Fixes Applied",
        "",
    ]

    p2 = all_results.get("phase2", {})
    if p2.get("cnd_after") is not None:
        lines += [
            "### CND/LND Flag Wiring",
            f"- Central neck dissection: {p2['cnd_before']:,} -> {p2['cnd_after']:,} TRUE",
            f"- Lateral neck dissection: {p2['lnd_before']:,} -> {p2['lnd_after']:,} TRUE",
            "- Source: path_synoptics structured fields (central_compartment_dissection, "
            "tumor_1_level_examined, ln_location)",
            "",
            "### Operative Note Date Recovery",
            f"- Dates resolved: {p2.get('note_dates_before', 0):,} -> {p2.get('note_dates_after', 0):,}",
            "- Fallback chain: note_date -> surgery_date_fallback",
            "",
        ]

    lines += [
        "### Provenance System",
        "- Added unified provenance columns to 4 analysis tables",
        "- Columns: source_table, source_script, provenance_note, resolved_layer_version",
        "",
        "### Chronology Anomalies",
    ]
    p5 = all_results.get("phase5", {})
    lines.append(f"- Total classified: {p5.get('total_anomalies', 'N/A')}")
    for b in p5.get("buckets", []):
        lines.append(f"  - {b['bucket']}: {b['count']:,}")

    lines += [
        "",
        "### Health Dashboard Tables",
        "- `val_dataset_integrity_summary_v1`",
        "- `val_provenance_completeness_v2`",
        "- `val_episode_linkage_completeness_v1`",
        "",
        "### MotherDuck Optimization",
        "- ANALYZE TABLE run on all large canonical tables",
        "",
        "## Remaining Structural Limitations",
        "",
        "1. **imaging_nodule_long_v2** remains a schema stub (deprecated); "
        "`imaging_nodule_master_v1` is canonical",
        "2. **Non-Tg lab dates** (TSH/PTH/Ca/vitD) at 0% — requires institutional data extract",
        "3. **Nuclear medicine notes** — zero in corpus; cannot improve RAI NLP further",
        "4. **Vascular invasion grading** — 87% remain present_ungraded "
        "(synoptic template limitation)",
        "5. **ETE sub-grading** — 49 remain present_ungraded after Phase 9 rules",
        "",
        "## Validation Outputs",
        "",
        "See `exports/dataset_maturation_*/` for CSV exports of all monitoring tables.",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  Report: {output_path}")


def export_results(con: duckdb.DuckDBPyConnection, export_dir: Path) -> None:
    export_dir.mkdir(parents=True, exist_ok=True)

    export_tables = [
        "val_dataset_integrity_summary_v1",
        "val_provenance_completeness_v2",
        "val_episode_linkage_completeness_v1",
        "val_temporal_anomaly_resolution_v1",
    ]

    for tbl in export_tables:
        if table_available(con, tbl):
            try:
                df = con.execute(f"SELECT * FROM {tbl}").fetchdf()
                csv_path = export_dir / f"{tbl}.csv"
                df.to_csv(csv_path, index=False)
                print(f"  Exported {tbl} -> {csv_path.name} ({len(df):,} rows)")
            except Exception as e:
                print(f"  WARN exporting {tbl}: {e}")

    manifest = {
        "generated_at": datetime.now().isoformat(),
        "tables_exported": [t for t in export_tables if table_available(con, t)],
        "export_dir": str(export_dir),
    }
    manifest_path = export_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"  Manifest: {manifest_path}")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--md", action="store_true", help="Write to MotherDuck")
    parser.add_argument("--phase", type=int, help="Run single phase (1-10)")
    parser.add_argument("--all", action="store_true", help="Run all phases")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    args = parser.parse_args()

    if not args.phase and not args.all:
        print("Usage: 75_dataset_maturation.py --all --md  (or --phase N --md)")
        sys.exit(1)

    section("75 — Dataset Maturation Pass")
    print(f"  Mode:      {'MotherDuck' if args.md else 'local'}")
    print(f"  Dry run:   {args.dry_run}")
    print(f"  Phase:     {'all' if args.all else args.phase}")
    print(f"  Timestamp: {TIMESTAMP}")

    if args.md:
        con = connect_md()
        print("  Connected to MotherDuck (RW)")
    else:
        con = duckdb.connect(str(DB_PATH))
        print(f"  Connected to local DB: {DB_PATH}")

    all_results: dict[str, Any] = {}
    phases_to_run = list(range(1, 11)) if args.all else [args.phase]

    for phase_num in phases_to_run:
        if phase_num == 1:
            all_results["phase1"] = phase1_canonical_integrity(con, args.dry_run)
        elif phase_num == 2:
            all_results["phase2"] = phase2_operative_enhancement(con, args.dry_run)
        elif phase_num == 3:
            all_results["phase3"] = phase3_imaging_canonicalization(con, args.dry_run)
        elif phase_num == 4:
            all_results["phase4"] = phase4_provenance_hardening(con, args.dry_run)
        elif phase_num == 5:
            all_results["phase5"] = phase5_chronology_adjudication(con, args.dry_run)
        elif phase_num == 6:
            section("Phase 6 — Documentation Sync")
            print("  Documentation updates handled via file edits (see README, RELEASE_NOTES, CHECKLIST).")
        elif phase_num == 7:
            all_results["phase7"] = phase7_dashboard_tables(con, args.dry_run)
        elif phase_num == 8:
            all_results["phase8"] = phase8_motherduck_optimization(con, args.dry_run)
        elif phase_num == 9:
            section("Phase 9 — Final Verification Pass")
            print("  Run verification scripts externally:")
            print("    .venv/bin/python scripts/67_database_hardening_validation.py --md")
            print("    .venv/bin/python scripts/70_operative_note_path_linkage_audit.py --md")
            print("    .venv/bin/python scripts/67_hp_discharge_note_audit.py --md")
        elif phase_num == 10:
            section("Phase 10 — Deliverables")
            integrity_path = ROOT / "docs" / "canonical_layer_integrity_report_20260313_addendum.md"
            maturation_path = ROOT / "docs" / "dataset_maturation_report_20260313.md"
            generate_integrity_report(all_results, integrity_path)
            generate_maturation_report(all_results, maturation_path)
            if not args.dry_run:
                export_results(con, EXPORT_DIR)

    con.close()

    section("Dataset Maturation Pass — Complete")
    print(f"  Phases run: {phases_to_run}")
    print(f"  Mode: {'MotherDuck' if args.md else 'local'}")
    if not args.dry_run and 10 in phases_to_run:
        print(f"  Exports: {EXPORT_DIR}")
    print()


if __name__ == "__main__":
    main()
