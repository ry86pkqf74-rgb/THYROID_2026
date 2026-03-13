#!/usr/bin/env python3
"""
76_canonical_gap_closure.py -- Close remaining canonical table propagation gaps

Phases:
  A: Operative NLP enrichment (parathyroid, frozen section, berry ligament, EBL)
  B: RAI dose provenance columns + relaxed date matching
  C: Molecular RAS subtype propagation
  D: Linkage ID propagation from v3 linkage tables into canonical tables
  E: Recurrence date hardening with quality tiers

Supports --md (MotherDuck), --local, --dry-run, --phase A/B/C/D/E.
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
        return int(con.execute(sql).fetchone()[0])
    except Exception:
        return -1


def fill_rate(con: duckdb.DuckDBPyConnection, tbl: str, col: str) -> dict[str, Any]:
    total = safe_count(con, f"SELECT COUNT(*) FROM {tbl}")
    filled = safe_count(con, f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NOT NULL")
    pct = round(100.0 * filled / total, 2) if total > 0 else 0.0
    return {"table": tbl, "column": col, "filled": filled, "total": total, "pct": pct}


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


# ═══════════════════════════════════════════════════════════════════════
# PHASE A — Operative NLP Enrichment
# ═══════════════════════════════════════════════════════════════════════

OP_ADD_COLS_SQL = [
    "ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS parathyroid_identified_count INTEGER;",
    "ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS parathyroid_resection_flag BOOLEAN;",
    "ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS frozen_section_flag BOOLEAN;",
    "ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS berry_ligament_flag BOOLEAN;",
    "ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS ebl_ml_nlp DOUBLE;",
    "ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS op_enrichment_source VARCHAR;",
]

OP_PARATHYROID_COUNT_SQL = """
UPDATE operative_episode_detail_v2 o
SET parathyroid_identified_count = src.para_count
FROM (
    SELECT
        ne.research_id,
        MAX(TRY_CAST(regexp_extract(ne.entity_value_raw, '(\\d+)', 1) AS INTEGER)) AS para_count
    FROM note_entities_procedures ne
    WHERE ne.entity_value_norm IN ('parathyroid_count_identified', 'parathyroid_identified')
      AND ne.present_or_negated = 'present'
    GROUP BY ne.research_id
) src
WHERE o.research_id = src.research_id
  AND o.parathyroid_identified_count IS NULL
  AND src.para_count IS NOT NULL
"""

OP_PARATHYROID_RESECTION_SQL = """
UPDATE operative_episode_detail_v2 o
SET parathyroid_resection_flag = TRUE
FROM (
    SELECT DISTINCT ne.research_id
    FROM note_entities_procedures ne
    WHERE ne.entity_value_norm IN ('parathyroid_removed', 'parathyroid_resection')
      AND ne.present_or_negated = 'present'
) src
WHERE o.research_id = src.research_id
  AND o.parathyroid_resection_flag IS NULL
"""

OP_FROZEN_SECTION_SQL = """
UPDATE operative_episode_detail_v2 o
SET frozen_section_flag = TRUE
FROM (
    SELECT DISTINCT ne.research_id
    FROM note_entities_procedures ne
    WHERE ne.entity_value_norm IN ('frozen_section_sent', 'frozen_section_result')
      AND ne.present_or_negated = 'present'
) src
WHERE o.research_id = src.research_id
  AND o.frozen_section_flag IS NULL
"""

OP_BERRY_LIGAMENT_SQL = """
UPDATE operative_episode_detail_v2 o
SET berry_ligament_flag = TRUE
FROM (
    SELECT DISTINCT ne.research_id
    FROM note_entities_procedures ne
    WHERE ne.entity_value_norm LIKE 'berry_ligament%'
      AND ne.present_or_negated = 'present'
) src
WHERE o.research_id = src.research_id
  AND o.berry_ligament_flag IS NULL
"""

OP_EBL_SQL = """
UPDATE operative_episode_detail_v2 o
SET ebl_ml_nlp = src.ebl_val
FROM (
    SELECT
        ne.research_id,
        MAX(TRY_CAST(regexp_extract(ne.entity_value_raw, '(\\d+\\.?\\d*)', 1) AS DOUBLE)) AS ebl_val
    FROM note_entities_procedures ne
    WHERE ne.entity_type = 'ebl'
      AND ne.present_or_negated = 'present'
    GROUP BY ne.research_id
) src
WHERE o.research_id = src.research_id
  AND o.ebl_ml_nlp IS NULL
  AND src.ebl_val IS NOT NULL
  AND src.ebl_val BETWEEN 1 AND 5000
"""

OP_PROVENANCE_SQL = """
UPDATE operative_episode_detail_v2
SET op_enrichment_source = 'nlp_extract_operative_v2'
WHERE op_enrichment_source IS NULL
  AND (parathyroid_identified_count IS NOT NULL
       OR parathyroid_resection_flag IS TRUE
       OR frozen_section_flag IS TRUE
       OR berry_ligament_flag IS TRUE
       OR ebl_ml_nlp IS NOT NULL)
"""


def phase_a_operative(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase A: Operative NLP Enrichment")
    metrics: dict[str, Any] = {"before": {}, "after": {}}

    before_count = safe_count(con, "SELECT COUNT(*) FROM operative_episode_detail_v2")
    metrics["before"]["row_count"] = before_count

    for sql in OP_ADD_COLS_SQL:
        if dry:
            print(f"  [DRY] {sql[:80]}...")
        else:
            con.execute(sql)

    steps = [
        ("parathyroid_identified_count", OP_PARATHYROID_COUNT_SQL),
        ("parathyroid_resection_flag", OP_PARATHYROID_RESECTION_SQL),
        ("frozen_section_flag", OP_FROZEN_SECTION_SQL),
        ("berry_ligament_flag", OP_BERRY_LIGAMENT_SQL),
        ("ebl_ml_nlp", OP_EBL_SQL),
        ("op_enrichment_source", OP_PROVENANCE_SQL),
    ]

    for col, sql in steps:
        before = fill_rate(con, "operative_episode_detail_v2", col)
        metrics["before"][col] = before["pct"]
        if dry:
            print(f"  [DRY] UPDATE {col}: skipped")
        else:
            con.execute(sql)
        after = fill_rate(con, "operative_episode_detail_v2", col)
        metrics["after"][col] = after["pct"]
        delta = after["filled"] - before["filled"]
        print(f"  {col}: {before['filled']} -> {after['filled']} (+{delta}), {after['pct']}%")

    after_count = safe_count(con, "SELECT COUNT(*) FROM operative_episode_detail_v2")
    metrics["after"]["row_count"] = after_count
    assert after_count == before_count, f"Row count changed: {before_count} -> {after_count}"
    print(f"\n  Row count verified: {after_count} (unchanged)")
    return metrics


# ═══════════════════════════════════════════════════════════════════════
# PHASE B — RAI Dose Provenance + Relaxed Matching
# ═══════════════════════════════════════════════════════════════════════

RAI_ADD_COLS_SQL = [
    "ALTER TABLE rai_treatment_episode_v2 ADD COLUMN IF NOT EXISTS dose_source VARCHAR;",
    "ALTER TABLE rai_treatment_episode_v2 ADD COLUMN IF NOT EXISTS dose_confidence DOUBLE;",
    "ALTER TABLE rai_treatment_episode_v2 ADD COLUMN IF NOT EXISTS surgery_link_score_v3 DOUBLE;",
]

RAI_DOSE_PROVENANCE_SQL = """
UPDATE rai_treatment_episode_v2
SET dose_source = 'extracted_rai_dose_refined_v1',
    dose_confidence = 0.85
WHERE dose_mci IS NOT NULL
  AND dose_source IS NULL
"""

RAI_DOSE_RELAXED_SQL = """
UPDATE rai_treatment_episode_v2 r
SET dose_mci = src.dose_mci,
    dose_source = src.source_table,
    dose_confidence = CAST(src.source_reliability AS DOUBLE)
FROM (
    SELECT
        d.research_id,
        d.dose_mci,
        d.source_table,
        d.source_reliability,
        d.rai_date,
        ROW_NUMBER() OVER (
            PARTITION BY d.research_id
            ORDER BY d.source_reliability DESC, d.dose_mci DESC
        ) AS rn
    FROM extracted_rai_dose_refined_v1 d
) src
WHERE r.research_id = src.research_id
  AND src.rn = 1
  AND r.dose_mci IS NULL
  AND src.dose_mci IS NOT NULL
"""

RAI_LINK_SCORE_SQL = """
UPDATE rai_treatment_episode_v2 r
SET surgery_link_score_v3 = CAST(src.linkage_score AS DOUBLE)
FROM (
    SELECT research_id, linkage_score,
           ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY linkage_score DESC) AS rn
    FROM pathology_rai_linkage_v3
) src
WHERE r.research_id = src.research_id
  AND src.rn = 1
  AND r.surgery_link_score_v3 IS NULL
"""


def phase_b_rai(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase B: RAI Dose Provenance + Relaxed Matching")
    metrics: dict[str, Any] = {"before": {}, "after": {}}

    before_count = safe_count(con, "SELECT COUNT(*) FROM rai_treatment_episode_v2")
    for col in ["dose_mci", "dose_source", "dose_confidence", "surgery_link_score_v3"]:
        metrics["before"][col] = fill_rate(con, "rai_treatment_episode_v2", col)["pct"]

    for sql in RAI_ADD_COLS_SQL:
        if dry:
            print(f"  [DRY] {sql[:80]}...")
        else:
            con.execute(sql)

    steps = [
        ("dose_source (provenance)", RAI_DOSE_PROVENANCE_SQL),
        ("dose_mci (relaxed)", RAI_DOSE_RELAXED_SQL),
        ("surgery_link_score_v3", RAI_LINK_SCORE_SQL),
    ]
    for label, sql in steps:
        if dry:
            print(f"  [DRY] {label}: skipped")
        else:
            con.execute(sql)

    for col in ["dose_mci", "dose_source", "dose_confidence", "surgery_link_score_v3"]:
        after = fill_rate(con, "rai_treatment_episode_v2", col)
        metrics["after"][col] = after["pct"]
        before_pct = metrics["before"][col]
        print(f"  {col}: {before_pct}% -> {after['pct']}% ({after['filled']}/{after['total']})")

    after_count = safe_count(con, "SELECT COUNT(*) FROM rai_treatment_episode_v2")
    assert after_count == before_count, f"Row count changed: {before_count} -> {after_count}"
    print(f"\n  Row count verified: {after_count} (unchanged)")
    return metrics


# ═══════════════════════════════════════════════════════════════════════
# PHASE C — Molecular RAS Subtype Propagation
# ═══════════════════════════════════════════════════════════════════════

MOL_RAS_SUBTYPE_SQL = """
UPDATE molecular_test_episode_v2 m
SET ras_subtype = src.ras_primary_subtype
FROM (
    SELECT research_id, ras_primary_subtype
    FROM extracted_ras_patient_summary_v1
    WHERE ras_primary_subtype IS NOT NULL
      AND TRIM(ras_primary_subtype) != ''
      AND ras_primary_subtype != 'RAS_unspecified'
) src
WHERE m.research_id = src.research_id
  AND m.ras_flag IS TRUE
  AND (m.ras_subtype IS NULL OR TRIM(m.ras_subtype) = '')
"""

MOL_FNA_SCORE_SQL = """
UPDATE molecular_test_episode_v2 m
SET fna_link_score_v3 = CAST(src.linkage_score AS DOUBLE)
FROM (
    SELECT
        research_id,
        linkage_score,
        ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY linkage_score DESC) AS rn
    FROM fna_molecular_linkage_v3
) src
WHERE m.research_id = src.research_id
  AND src.rn = 1
  AND m.fna_link_score_v3 IS NULL
"""


def phase_c_molecular(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase C: Molecular RAS Subtype Propagation")
    metrics: dict[str, Any] = {"before": {}, "after": {}}

    con.execute("ALTER TABLE molecular_test_episode_v2 ADD COLUMN IF NOT EXISTS fna_link_score_v3 DOUBLE;")

    before_count = safe_count(con, "SELECT COUNT(*) FROM molecular_test_episode_v2")
    for col in ["ras_subtype", "fna_link_score_v3"]:
        metrics["before"][col] = fill_rate(con, "molecular_test_episode_v2", col)["pct"]

    if not dry:
        con.execute(MOL_RAS_SUBTYPE_SQL)
        con.execute(MOL_FNA_SCORE_SQL)
    else:
        print("  [DRY] RAS subtype + FNA score: skipped")

    for col in ["ras_subtype", "ras_flag", "braf_flag", "fna_link_score_v3"]:
        after = fill_rate(con, "molecular_test_episode_v2", col)
        metrics["after"][col] = after["pct"]
        print(f"  {col}: {after['filled']}/{after['total']} = {after['pct']}%")

    after_count = safe_count(con, "SELECT COUNT(*) FROM molecular_test_episode_v2")
    assert after_count == before_count, f"Row count changed: {before_count} -> {after_count}"
    print(f"\n  Row count verified: {after_count} (unchanged)")
    return metrics


# ═══════════════════════════════════════════════════════════════════════
# PHASE D — Linkage ID Propagation
# ═══════════════════════════════════════════════════════════════════════

TUMOR_LINK_ADD_SQL = [
    "ALTER TABLE tumor_episode_master_v2 ADD COLUMN IF NOT EXISTS linked_surgery_episode_id VARCHAR;",
    "ALTER TABLE tumor_episode_master_v2 ADD COLUMN IF NOT EXISTS surgery_link_score_v3 DOUBLE;",
    "ALTER TABLE tumor_episode_master_v2 ADD COLUMN IF NOT EXISTS surgery_link_tier VARCHAR;",
]

TUMOR_LINK_UPDATE_SQL = """
UPDATE tumor_episode_master_v2 t
SET linked_surgery_episode_id = CAST(src.surgery_episode_id AS VARCHAR),
    surgery_link_score_v3 = CAST(src.linkage_score AS DOUBLE),
    surgery_link_tier = src.linkage_confidence_tier
FROM (
    SELECT research_id, surgery_episode_id, linkage_score, linkage_confidence_tier,
           ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY linkage_score DESC) AS rn
    FROM surgery_pathology_linkage_v3
) src
WHERE t.research_id = src.research_id
  AND src.rn = 1
  AND t.linked_surgery_episode_id IS NULL
"""

OP_LINK_ADD_SQL = [
    "ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS linked_pathology_episode_id VARCHAR;",
    "ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS path_link_score_v3 DOUBLE;",
    "ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS linked_fna_episode_id VARCHAR;",
    "ALTER TABLE operative_episode_detail_v2 ADD COLUMN IF NOT EXISTS fna_link_score_v3 DOUBLE;",
]

OP_PATH_LINK_SQL = """
UPDATE operative_episode_detail_v2 o
SET linked_pathology_episode_id = CAST(src.path_surgery_id AS VARCHAR),
    path_link_score_v3 = CAST(src.linkage_score AS DOUBLE)
FROM (
    SELECT research_id, surgery_episode_id, path_surgery_id, linkage_score,
           ROW_NUMBER() OVER (PARTITION BY research_id, surgery_episode_id ORDER BY linkage_score DESC) AS rn
    FROM surgery_pathology_linkage_v3
) src
WHERE o.research_id = src.research_id
  AND o.surgery_episode_id = src.surgery_episode_id
  AND src.rn = 1
  AND o.linked_pathology_episode_id IS NULL
"""

OP_FNA_LINK_SQL = """
UPDATE operative_episode_detail_v2 o
SET linked_fna_episode_id = CAST(src.preop_episode_id AS VARCHAR),
    fna_link_score_v3 = CAST(src.linkage_score AS DOUBLE)
FROM (
    SELECT research_id, surgery_episode_id, preop_episode_id, linkage_score,
           ROW_NUMBER() OVER (PARTITION BY research_id, surgery_episode_id ORDER BY linkage_score DESC) AS rn
    FROM preop_surgery_linkage_v3
) src
WHERE o.research_id = src.research_id
  AND o.surgery_episode_id = src.surgery_episode_id
  AND src.rn = 1
  AND o.linked_fna_episode_id IS NULL
"""

IMG_LINK_ADD_SQL = [
    "ALTER TABLE imaging_nodule_master_v1 ADD COLUMN IF NOT EXISTS linked_fna_episode_id VARCHAR;",
    "ALTER TABLE imaging_nodule_master_v1 ADD COLUMN IF NOT EXISTS fna_link_score_v3 DOUBLE;",
]

IMG_LINK_SQL = """
UPDATE imaging_nodule_master_v1 i
SET linked_fna_episode_id = CAST(src.fna_episode_id AS VARCHAR),
    fna_link_score_v3 = CAST(src.linkage_score AS DOUBLE)
FROM (
    SELECT research_id, fna_episode_id, linkage_score,
           ROW_NUMBER() OVER (PARTITION BY research_id ORDER BY linkage_score DESC) AS rn
    FROM imaging_fna_linkage_v3
    WHERE linkage_score IS NOT NULL
) src
WHERE CAST(i.research_id AS VARCHAR) = CAST(src.research_id AS VARCHAR)
  AND src.rn = 1
  AND i.linked_fna_episode_id IS NULL
"""

FNA_LINK_ADD_SQL = [
    "ALTER TABLE fna_episode_master_v2 ADD COLUMN IF NOT EXISTS linked_surgery_episode_id VARCHAR;",
    "ALTER TABLE fna_episode_master_v2 ADD COLUMN IF NOT EXISTS surgery_link_score_v3 DOUBLE;",
]

FNA_SURG_LINK_SQL = """
UPDATE fna_episode_master_v2 f
SET linked_surgery_episode_id = CAST(src.surgery_episode_id AS VARCHAR),
    surgery_link_score_v3 = CAST(src.linkage_score AS DOUBLE)
FROM (
    SELECT research_id, preop_episode_id, surgery_episode_id, linkage_score,
           ROW_NUMBER() OVER (PARTITION BY research_id, preop_episode_id ORDER BY linkage_score DESC) AS rn
    FROM preop_surgery_linkage_v3
) src
WHERE f.research_id = src.research_id
  AND f.fna_episode_id = src.preop_episode_id
  AND src.rn = 1
  AND f.linked_surgery_episode_id IS NULL
"""


def phase_d_linkage(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase D: Linkage ID Propagation")
    metrics: dict[str, Any] = {"before": {}, "after": {}}

    link_steps = [
        ("tumor_episode_master_v2", TUMOR_LINK_ADD_SQL, [
            ("linked_surgery_episode_id", TUMOR_LINK_UPDATE_SQL),
        ]),
        ("operative_episode_detail_v2", OP_LINK_ADD_SQL, [
            ("linked_pathology_episode_id", OP_PATH_LINK_SQL),
            ("linked_fna_episode_id", OP_FNA_LINK_SQL),
        ]),
        ("imaging_nodule_master_v1", IMG_LINK_ADD_SQL, [
            ("linked_fna_episode_id", IMG_LINK_SQL),
        ]),
        ("fna_episode_master_v2", FNA_LINK_ADD_SQL, [
            ("linked_surgery_episode_id", FNA_SURG_LINK_SQL),
        ]),
    ]

    for tbl, add_sqls, updates in link_steps:
        print(f"\n  --- {tbl} ---")
        before_count = safe_count(con, f"SELECT COUNT(*) FROM {tbl}")

        for sql in add_sqls:
            if dry:
                print(f"    [DRY] {sql[:70]}...")
            else:
                con.execute(sql)

        for col, sql in updates:
            before = fill_rate(con, tbl, col)
            if dry:
                print(f"    [DRY] {col}: skipped")
            else:
                con.execute(sql)
            after = fill_rate(con, tbl, col)
            delta = after["filled"] - before["filled"]
            print(f"    {col}: {before['filled']} -> {after['filled']} (+{delta}), {after['pct']}%")
            metrics[f"{tbl}.{col}"] = {"before": before["pct"], "after": after["pct"]}

        after_count = safe_count(con, f"SELECT COUNT(*) FROM {tbl}")
        assert after_count == before_count, f"{tbl} row count changed: {before_count} -> {after_count}"
        print(f"    Row count: {after_count} (unchanged)")

    return metrics


# ═══════════════════════════════════════════════════════════════════════
# PHASE E — Recurrence Date Hardening
# ═══════════════════════════════════════════════════════════════════════

RECURRENCE_ADD_COLS_SQL = [
    "ALTER TABLE extracted_recurrence_refined_v1 ADD COLUMN IF NOT EXISTS recurrence_date_best DATE;",
    "ALTER TABLE extracted_recurrence_refined_v1 ADD COLUMN IF NOT EXISTS recurrence_date_status VARCHAR;",
    "ALTER TABLE extracted_recurrence_refined_v1 ADD COLUMN IF NOT EXISTS recurrence_date_confidence DOUBLE;",
]

RECURRENCE_DATE_TIER_SQL = """
UPDATE extracted_recurrence_refined_v1
SET
    recurrence_date_best = CASE
        WHEN TRY_CAST(first_recurrence_date AS DATE) IS NOT NULL
            THEN TRY_CAST(first_recurrence_date AS DATE)
        WHEN tg_rising_flag IS TRUE AND detection_category = 'biochemical_only'
            THEN NULL
        ELSE NULL
    END,
    recurrence_date_status = CASE
        WHEN TRY_CAST(first_recurrence_date AS DATE) IS NOT NULL
            THEN 'exact_source_date'
        WHEN tg_rising_flag IS TRUE AND detection_category = 'biochemical_only'
            THEN 'biochemical_inflection_inferred'
        WHEN detection_category IN ('structural_confirmed', 'structural_date_unknown')
            THEN 'unresolved_date'
        ELSE 'not_applicable'
    END,
    recurrence_date_confidence = CASE
        WHEN TRY_CAST(first_recurrence_date AS DATE) IS NOT NULL THEN 1.0
        WHEN tg_rising_flag IS TRUE AND detection_category = 'biochemical_only' THEN 0.5
        WHEN detection_category IN ('structural_confirmed', 'structural_date_unknown') THEN 0.0
        ELSE NULL
    END
WHERE recurrence_date_status IS NULL
"""


def phase_e_recurrence(con: duckdb.DuckDBPyConnection, dry: bool) -> dict:
    section("Phase E: Recurrence Date Hardening")
    metrics: dict[str, Any] = {"before": {}, "after": {}}

    before_count = safe_count(con, "SELECT COUNT(*) FROM extracted_recurrence_refined_v1")
    metrics["before"]["row_count"] = before_count

    for sql in RECURRENCE_ADD_COLS_SQL:
        if dry:
            print(f"  [DRY] {sql[:80]}...")
        else:
            con.execute(sql)

    if not dry:
        con.execute(RECURRENCE_DATE_TIER_SQL)

    for col in ["recurrence_date_best", "recurrence_date_status", "recurrence_date_confidence"]:
        after = fill_rate(con, "extracted_recurrence_refined_v1", col)
        metrics["after"][col] = after["pct"]
        print(f"  {col}: {after['filled']}/{after['total']} = {after['pct']}%")

    try:
        dist = con.execute("""
            SELECT recurrence_date_status, COUNT(*) as n
            FROM extracted_recurrence_refined_v1
            WHERE recurrence_date_status IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC
        """).fetchall()
        print("\n  Date status distribution:")
        for row in dist:
            print(f"    {row[0]}: {row[1]}")
    except Exception:
        pass

    after_count = safe_count(con, "SELECT COUNT(*) FROM extracted_recurrence_refined_v1")
    assert after_count == before_count, f"Row count changed: {before_count} -> {after_count}"
    print(f"\n  Row count verified: {after_count} (unchanged)")
    return metrics


# ═══════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Canonical gap closure")
    parser.add_argument("--md", action="store_true", help="Target MotherDuck")
    parser.add_argument("--local", action="store_true", help="Target local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    parser.add_argument("--phase", choices=["A", "B", "C", "D", "E", "all"], default="all")
    args = parser.parse_args()

    if not args.md and not args.local:
        args.local = True

    con = connect(args)
    dry = args.dry_run
    report: dict[str, Any] = {"timestamp": TIMESTAMP, "target": "md" if args.md else "local"}

    phases = {
        "A": ("Operative NLP Enrichment", phase_a_operative),
        "B": ("RAI Dose Provenance", phase_b_rai),
        "C": ("Molecular RAS Subtype", phase_c_molecular),
        "D": ("Linkage ID Propagation", phase_d_linkage),
        "E": ("Recurrence Date Hardening", phase_e_recurrence),
    }

    to_run = phases.keys() if args.phase == "all" else [args.phase]

    for p in to_run:
        name, func = phases[p]
        try:
            if table_exists(con, "operative_episode_detail_v2") or p not in ("A",):
                report[p] = func(con, dry)
            else:
                print(f"  Skipping Phase {p}: required table not found")
        except Exception as e:
            print(f"  Phase {p} ERROR: {e}")
            report[p] = {"error": str(e)}

    if not dry and args.md:
        section("Running ANALYZE on modified tables")
        for tbl in [
            "operative_episode_detail_v2",
            "rai_treatment_episode_v2",
            "molecular_test_episode_v2",
            "tumor_episode_master_v2",
            "fna_episode_master_v2",
            "imaging_nodule_master_v1",
            "extracted_recurrence_refined_v1",
        ]:
            try:
                con.execute(f"ANALYZE {tbl}")
                print(f"  ANALYZE {tbl}: done")
            except Exception as e:
                print(f"  ANALYZE {tbl}: {e}")

    report_path = ROOT / "docs" / f"canonical_gap_closure_report_{TIMESTAMP}.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nReport saved to {report_path}")

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
