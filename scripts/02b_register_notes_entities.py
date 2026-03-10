#!/usr/bin/env python3
"""
02b_register_notes_entities.py — Register notes + entity parquets in DuckDB

Registers:
  - clinical_notes_long
  - note_entities_staging
  - note_entities_genetics
  - note_entities_procedures
  - note_entities_complications
  - note_entities_medications
  - note_entities_problem_list

Creates views:
  - notes_entity_summary   (aggregated counts per patient)
  - advanced_features_v2   (extended with entity-availability flags)
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "processed"
DB_PATH = ROOT / "thyroid_master.duckdb"

ENTITY_TABLES = [
    "clinical_notes_long",
    "note_entities_staging",
    "note_entities_genetics",
    "note_entities_procedures",
    "note_entities_complications",
    "note_entities_medications",
    "note_entities_problem_list",
]

ENTITY_SUMMARY_SQL = """
CREATE OR REPLACE VIEW notes_entity_summary AS
WITH all_entities AS (
    SELECT research_id, 'staging' AS domain, entity_value_norm, present_or_negated
    FROM note_entities_staging
    UNION ALL
    SELECT research_id, 'genetics', entity_value_norm, present_or_negated
    FROM note_entities_genetics
    UNION ALL
    SELECT research_id, 'procedures', entity_value_norm, present_or_negated
    FROM note_entities_procedures
    UNION ALL
    SELECT research_id, 'complications', entity_value_norm, present_or_negated
    FROM note_entities_complications
    UNION ALL
    SELECT research_id, 'medications', entity_value_norm, present_or_negated
    FROM note_entities_medications
    UNION ALL
    SELECT research_id, 'problem_list', entity_value_norm, present_or_negated
    FROM note_entities_problem_list
)
SELECT
    CAST(research_id AS VARCHAR) AS research_id,
    COUNT(*) AS n_entities_total,
    SUM(CASE WHEN domain = 'staging' THEN 1 ELSE 0 END) AS n_staging,
    SUM(CASE WHEN domain = 'genetics' THEN 1 ELSE 0 END) AS n_genetics,
    SUM(CASE WHEN domain = 'procedures' THEN 1 ELSE 0 END) AS n_procedures,
    SUM(CASE WHEN domain = 'complications' THEN 1 ELSE 0 END) AS n_complications,
    SUM(CASE WHEN domain = 'medications' THEN 1 ELSE 0 END) AS n_medications,
    SUM(CASE WHEN domain = 'problem_list' THEN 1 ELSE 0 END) AS n_problems,
    SUM(CASE WHEN present_or_negated = 'present' THEN 1 ELSE 0 END) AS n_present,
    SUM(CASE WHEN present_or_negated = 'negated' THEN 1 ELSE 0 END) AS n_negated
FROM all_entities
GROUP BY research_id
"""

ADVANCED_V2_EXTENDED_SQL = """
CREATE OR REPLACE VIEW advanced_features_v2 AS
SELECT
    mc.research_id,
    mc.age_at_surgery,
    mc.sex,
    mc.surgery_date,
    tp.histology_1_type,
    tp.histology_1_overall_stage_ajcc8,
    tp.histology_1_largest_tumor_cm,
    bp.is_mng,
    bp.is_graves,
    bp.is_follicular_adenoma,
    bp.is_hashimoto,
    comp.rln_injury_vocal_cord_paralysis,
    comp.vocal_cord_status,
    comp.seroma,
    comp.hematoma,
    comp.hypocalcemia,
    comp.hypoparathyroidism,
    od.ebl,
    od.skin_to_skin_time,
    tp.braf_mutation_mentioned,
    tp.ras_mutation_mentioned,
    tp.ret_mutation_mentioned,
    tp.tert_mutation_mentioned,
    -- Data availability (Phase 6 tables)
    CASE WHEN comp.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_complications,
    CASE WHEN od.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_operative_details,
    CASE WHEN cn.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_clinical_notes,
    CASE WHEN cnl.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_clinical_notes_long,
    CASE WHEN ps.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_path_synoptics,
    CASE WHEN mt_agg.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_molecular_testing,
    CASE WHEN fh_agg.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_fna_history,
    CASE WHEN unt_agg.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_us_tirads,
    -- Entity extraction flags
    CASE WHEN nes.n_problems > 0 THEN TRUE ELSE FALSE END AS has_problem_mentions,
    CASE WHEN nes.n_procedures > 0 THEN TRUE ELSE FALSE END AS has_procedure_mentions,
    CASE WHEN nes.n_complications > 0 THEN TRUE ELSE FALSE END AS has_complication_mentions,
    CASE WHEN nes.n_genetics > 0 THEN TRUE ELSE FALSE END AS has_genetics_mentions,
    CASE WHEN nes.n_staging > 0 THEN TRUE ELSE FALSE END AS has_staging_mentions,
    CASE WHEN nes.n_medications > 0 THEN TRUE ELSE FALSE END AS has_medication_mentions,
    COALESCE(nes.n_entities_total, 0) AS n_entities_total,
    -- Base table flags
    mc.has_tumor_pathology,
    mc.has_benign_pathology,
    mc.has_fna_cytology,
    mc.has_ultrasound_reports,
    mc.has_ct_imaging,
    mc.has_mri_imaging,
    mc.has_nuclear_med,
    mc.has_thyroglobulin_labs,
    mc.has_anti_thyroglobulin_labs,
    mc.has_parathyroid
FROM master_cohort mc
LEFT JOIN tumor_pathology tp      ON mc.research_id = tp.research_id
LEFT JOIN benign_pathology bp     ON mc.research_id = bp.research_id
LEFT JOIN complications comp      ON mc.research_id = CAST(comp.research_id AS VARCHAR)
LEFT JOIN operative_details od    ON mc.research_id = CAST(od.research_id AS VARCHAR)
LEFT JOIN clinical_notes cn       ON mc.research_id = CAST(cn.research_id AS VARCHAR)
LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM clinical_notes_long) cnl
    ON mc.research_id = cnl.research_id
LEFT JOIN path_synoptics ps       ON mc.research_id = CAST(ps.research_id AS VARCHAR)
LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM molecular_testing) mt_agg
    ON mc.research_id = mt_agg.research_id
LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM fna_history) fh_agg
    ON mc.research_id = fh_agg.research_id
LEFT JOIN (SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id FROM us_nodules_tirads) unt_agg
    ON mc.research_id = unt_agg.research_id
LEFT JOIN notes_entity_summary nes
    ON mc.research_id = nes.research_id
"""


def main() -> None:
    print("=" * 70)
    print("  REGISTER NOTES & ENTITY TABLES IN DUCKDB")
    print("=" * 70)

    if not DB_PATH.exists():
        print(f"  DuckDB not found — creating {DB_PATH}")

    con = duckdb.connect(str(DB_PATH))

    for tbl in ENTITY_TABLES:
        pq = PROCESSED / f"{tbl}.parquet"
        if not pq.exists():
            print(f"  SKIP {tbl} — parquet not found")
            continue
        con.execute(
            f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM read_parquet('{pq}')"
        )
        cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  Loaded {tbl:40s}  {cnt:>8,} rows")

    try:
        con.execute(ENTITY_SUMMARY_SQL)
        cnt = con.execute("SELECT COUNT(*) FROM notes_entity_summary").fetchone()[0]
        print(f"\n  View notes_entity_summary: {cnt:,} patients with entities")
    except Exception as exc:
        print(f"  notes_entity_summary FAILED: {exc}")

    has_master = any(
        r[0] == "master_cohort"
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall()
    )
    if has_master:
        try:
            con.execute(ADVANCED_V2_EXTENDED_SQL)
            cnt = con.execute("SELECT COUNT(*) FROM advanced_features_v2").fetchone()[0]
            print(f"  View advanced_features_v2 (extended): {cnt:,} rows")
        except Exception as exc:
            print(f"  advanced_features_v2 FAILED: {exc}")
    else:
        print("  SKIP advanced_features_v2 — master_cohort not present (DVC parquets needed)")

    tables = con.execute("SHOW TABLES").fetchall()
    print(f"\n  Tables ({len(tables)}):")
    for (t,) in tables:
        cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"    {t:40s}  {cnt:>8,}")

    views = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_type = 'VIEW'"
    ).fetchall()
    if views:
        print(f"\n  Views ({len(views)}):")
        for (v,) in views:
            print(f"    {v}")

    con.close()
    print(f"\n{'=' * 70}")
    print("  DONE")
    print("=" * 70)


if __name__ == "__main__":
    main()
