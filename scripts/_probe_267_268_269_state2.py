#!/usr/bin/env python3
"""Deeper probes for v1_0 trailing gaps."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402


def main() -> int:
    con = connect_locked()

    # 1. Registry rows for relevant tables
    print("--- detail_table_registry_v1 rows for FNA & Bethesda feeders ---")
    for t in ('fna_cytology', 'extracted_fna_bethesda_v1', 'fna_episode_master_v2',
              'specimen_tumor_focus_v1', 'imaging_fna_linkage_v3'):
        row = con.execute(f"""
            SELECT detail_table_name, schema_name, grain, total_rows,
                   feeds_master_columns_normalized, feeds_master_columns_secondary
            FROM {PUBLICATION_DB}.manuscript_workspace.detail_table_registry_v1
            WHERE detail_table_name = '{t}'
        """).fetchall()
        print(f"\n  {t}: {len(row)} rows")
        for r in row:
            print(f"    grain={r[2]} total_rows={r[3]}")
            print(f"    feeds_normalized={r[4]}")
            print(f"    feeds_secondary={r[5]}")

    # 2. Verify 4 legacy cols mismatch counts
    print("\n--- legacy column contradictions ---")
    n_v7_mismatch = con.execute("""
        SELECT COUNT(*) FROM canonical_patient_master
        WHERE COALESCE(molecular_tested_v7, FALSE) <> COALESCE(molecular_tested_confirmed, FALSE)
    """).fetchone()[0]
    print(f"  molecular_tested_v7 vs molecular_tested_confirmed mismatches: {n_v7_mismatch}")

    n_test_count_mismatch = con.execute("""
        SELECT COUNT(*) FROM canonical_patient_master
        WHERE COALESCE(mol_test_count, -1) <> COALESCE(mol_n_tests, -1)
    """).fetchone()[0]
    n_test_count_null = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master WHERE mol_test_count IS NULL"
    ).fetchone()[0]
    print(f"  mol_test_count vs mol_n_tests mismatches: {n_test_count_mismatch}")
    print(f"  mol_test_count NULLs: {n_test_count_null}")

    # 3. F1 gap counts
    print("\n--- F1 gap analysis ---")
    # ThyroSeq gap: patients with mol_has_thyroseq=TRUE but no thyroseq mte row
    n_ts_gap = con.execute("""
        WITH ts_pts AS (
          SELECT DISTINCT cmt.research_id
          FROM canonical_molecular_tested_v1 cmt
          WHERE cmt.has_thyroseq = TRUE
        ),
        mte_ts AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
          FROM molecular_test_episode_v2
          WHERE LOWER(COALESCE(platform, '')) LIKE '%thyroseq%'
             OR LOWER(COALESCE(platform_raw, '')) LIKE '%thyroseq%'
        )
        SELECT COUNT(*) FROM ts_pts t
        WHERE NOT EXISTS (SELECT 1 FROM mte_ts m WHERE m.rid = t.research_id)
    """).fetchone()[0]
    print(f"  patients flagged has_thyroseq with NO thyroseq mte row: {n_ts_gap}")

    n_thyroseq_pts_in_enrichment = con.execute("""
        SELECT COUNT(DISTINCT research_id) FROM thyroseq_molecular_enrichment
    """).fetchone()[0]
    print(f"  distinct patients in thyroseq_molecular_enrichment: {n_thyroseq_pts_in_enrichment}")

    n_thyroseq_in_cpm_set = con.execute("""
        SELECT COUNT(DISTINCT te.research_id)
        FROM thyroseq_molecular_enrichment te
        JOIN canonical_patient_master cpm
          ON CAST(te.research_id AS VARCHAR) = cpm.research_id
    """).fetchone()[0]
    print(f"  thyroseq_enrichment patients in CPM: {n_thyroseq_in_cpm_set}")

    # ThyroSeq pts in enrichment that have NO mte_v2 thyroseq row
    n_ts_enrichment_gap = con.execute("""
        WITH te_pts AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
          FROM thyroseq_molecular_enrichment
        ),
        mte_ts AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
          FROM molecular_test_episode_v2
          WHERE LOWER(COALESCE(platform, '')) LIKE '%thyroseq%'
             OR LOWER(COALESCE(platform_raw, '')) LIKE '%thyroseq%'
        )
        SELECT COUNT(*) FROM te_pts t
        WHERE NOT EXISTS (SELECT 1 FROM mte_ts m WHERE m.rid = t.rid)
    """).fetchone()[0]
    print(f"  thyroseq_enrichment patients WITHOUT mte_v2 thyroseq row: {n_ts_enrichment_gap}")

    # NGS-BRAF gap
    print("\n--- NGS-BRAF gap ---")
    braf_methods = con.execute("""
        SELECT detection_method, COUNT(*) FROM extracted_braf_recovery_v1
        GROUP BY detection_method ORDER BY 2 DESC
    """).fetchall()
    print(f"  BRAF detection_methods: {braf_methods}")
    braf_status = con.execute("""
        SELECT braf_status, COUNT(*) FROM extracted_braf_recovery_v1
        GROUP BY braf_status ORDER BY 2 DESC
    """).fetchall()
    print(f"  BRAF status distribution: {braf_status}")

    n_braf_ngs_pos = con.execute("""
        SELECT COUNT(DISTINCT research_id) FROM extracted_braf_recovery_v1
        WHERE UPPER(COALESCE(braf_status, '')) = 'POSITIVE'
          AND UPPER(COALESCE(detection_method, '')) LIKE '%NGS%'
    """).fetchone()[0]
    print(f"  Distinct patients NGS+BRAF positive: {n_braf_ngs_pos}")

    n_braf_ngs_gap = con.execute("""
        WITH src AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
          FROM extracted_braf_recovery_v1
          WHERE UPPER(COALESCE(braf_status, '')) = 'POSITIVE'
            AND UPPER(COALESCE(detection_method, '')) LIKE '%NGS%'
        ),
        existing AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
          FROM molecular_test_episode_v2
          WHERE braf_flag = TRUE
        )
        SELECT COUNT(*) FROM src s
        WHERE NOT EXISTS (SELECT 1 FROM existing e WHERE e.rid = s.rid)
    """).fetchone()[0]
    print(f"  NGS-BRAF positive patients WITHOUT mte braf_flag row: {n_braf_ngs_gap}")

    # RET gap
    print("\n--- RET gap ---")
    n_ret_pos = con.execute("""
        SELECT COUNT(DISTINCT research_id) FROM ret_patient_adjudicated_v226
        WHERE ret_note_true_positive = TRUE
    """).fetchone()[0]
    print(f"  RET adjudicated positive patients: {n_ret_pos}")
    n_ret_gap = con.execute("""
        WITH src AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
          FROM ret_patient_adjudicated_v226
          WHERE ret_note_true_positive = TRUE
        ),
        existing AS (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid
          FROM molecular_test_episode_v2
          WHERE ret_flag = TRUE OR ret_fusion_flag = TRUE
        )
        SELECT COUNT(*) FROM src s
        WHERE NOT EXISTS (SELECT 1 FROM existing e WHERE e.rid = s.rid)
    """).fetchone()[0]
    print(f"  RET positive patients WITHOUT mte ret_flag row: {n_ret_gap}")

    # Check what 'first_surgery_date' is in CPM
    print("\n--- first_surgery_date in CPM ---")
    n_fsd = con.execute("""
        SELECT COUNT(*) FROM canonical_patient_master WHERE first_surgery_date IS NOT NULL
    """).fetchone()[0]
    print(f"  CPM patients with non-null first_surgery_date: {n_fsd}")

    # Check fna_cytology dates
    print("\n--- FNA cytology data quality ---")
    fna_stats = con.execute("""
        SELECT
          COUNT(*) AS n_total,
          COUNT(DISTINCT research_id) AS n_pts,
          COUNT(*) FILTER (WHERE TRY_CAST(fna_date AS DATE) IS NOT NULL) AS n_dated,
          COUNT(*) FILTER (WHERE category_num BETWEEN 1 AND 6) AS n_cat,
          COUNT(*) FILTER (WHERE TRY_CAST(original_bethesda AS INTEGER) BETWEEN 1 AND 6) AS n_orig
        FROM fna_cytology
    """).fetchone()
    print(f"  FNA: total={fna_stats[0]}  pts={fna_stats[1]}  dated={fna_stats[2]}  "
          f"cat_num_valid={fna_stats[3]}  orig_int_valid={fna_stats[4]}")

    # Estimate preop FNA patient coverage
    preop_pts = con.execute("""
        SELECT COUNT(DISTINCT fc.research_id)
        FROM fna_cytology fc
        JOIN canonical_patient_master cpm ON fc.research_id = cpm.research_id
        WHERE TRY_CAST(fc.fna_date AS DATE) IS NOT NULL
          AND cpm.first_surgery_date IS NOT NULL
          AND TRY_CAST(fc.fna_date AS DATE) < cpm.first_surgery_date
          AND (fc.category_num BETWEEN 1 AND 6
               OR TRY_CAST(fc.original_bethesda AS INTEGER) BETWEEN 1 AND 6)
    """).fetchone()[0]
    print(f"  preop FNA patients with valid bethesda: {preop_pts}")

    # 4. specimen_tumor_focus_v1 / imaging_fna_linkage_v3 / fna_episode_master_v2 schema
    for t in ('specimen_tumor_focus_v1', 'imaging_fna_linkage_v3', 'fna_episode_master_v2'):
        try:
            cols = con.execute(f"""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                  AND table_name='{t}'
                ORDER BY ordinal_position
            """).fetchall()
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"\n  {t}: {n} rows, {len(cols)} cols")
            for c, ty in cols:
                print(f"    {c:<40} {ty}")
        except Exception as e:
            print(f"\n  {t}: ERROR {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
