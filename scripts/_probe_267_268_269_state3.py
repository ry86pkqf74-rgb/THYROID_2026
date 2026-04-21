#!/usr/bin/env python3
"""Final probes before writing scripts 267-269."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402


def main() -> int:
    con = connect_locked()

    # 1. molecular_episode_id behavior - sequence or manual?
    print("--- molecular_episode_id PK analysis ---")
    pk_stats = con.execute("""
        SELECT MIN(molecular_episode_id), MAX(molecular_episode_id),
               COUNT(*), COUNT(DISTINCT molecular_episode_id),
               COUNT(*) FILTER (WHERE molecular_episode_id IS NULL) AS n_null
        FROM molecular_test_episode_v2
    """).fetchone()
    print(f"  min={pk_stats[0]} max={pk_stats[1]} n_rows={pk_stats[2]} "
          f"n_distinct={pk_stats[3]} n_null={pk_stats[4]}")

    # Distinct molecular_episode_id values
    pk_dist = con.execute("""
        SELECT molecular_episode_id, COUNT(*) FROM molecular_test_episode_v2
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    print(f"  PK value distribution: {pk_dist}")
    seqs = con.execute("SELECT sequence_name FROM duckdb_sequences()").fetchall()
    print(f"  duckdb sequences: {seqs}")

    # 2. NOT NULL constraints on mte_v2
    print("\n--- mte_v2 actual nullability ---")
    nul = con.execute("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema='main' AND table_name='molecular_test_episode_v2'
          AND (is_nullable = 'NO' OR column_default IS NOT NULL)
        ORDER BY ordinal_position
    """).fetchall()
    print(f"  NOT NULL or DEFAULT'd columns: {nul}")

    # 3. fna_episode_master_v2 join key match
    print("\n--- fna_episode_master_v2 / fna_cytology join check ---")
    # fna_cytology has (research_id VARCHAR, fna_index BIGINT)
    # fna_episode_master_v2 has (fna_episode_id BIGINT, research_id VARCHAR, ...)
    # No 'fna_index' on fem_v2. How do we link?
    fem_cols = con.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='main' AND table_name='fna_episode_master_v2'
    """).fetchall()
    print(f"  fem_v2 cols: {[c[0] for c in fem_cols]}")
    # Sample to understand mapping
    sample = con.execute("""
        SELECT research_id, fna_episode_id, fna_date_native, resolved_fna_date,
               source_table FROM fna_episode_master_v2 LIMIT 5
    """).fetchall()
    print(f"  fem_v2 sample: {sample}")

    # 4. Date format check — what do unparseable values look like?
    print("\n--- fna_cytology.fna_date format probe ---")
    samples = con.execute("""
        SELECT fna_date, COUNT(*) FROM fna_cytology
        WHERE fna_date IS NOT NULL
          AND TRY_CAST(fna_date AS DATE) IS NULL
        GROUP BY fna_date ORDER BY 2 DESC LIMIT 10
    """).fetchall()
    print(f"  unparseable date samples: {samples}")

    # 5. Test the multi-format COALESCE approach
    print("\n--- multi-format date parse test ---")
    parsed_stats = con.execute("""
        SELECT
          COUNT(*) AS total,
          COUNT(*) FILTER (WHERE TRY_CAST(fna_date AS DATE) IS NOT NULL) AS iso,
          COUNT(*) FILTER (WHERE TRY_STRPTIME(fna_date, '%-m/%-d/%Y') IS NOT NULL) AS m_d_yyyy,
          COUNT(*) FILTER (WHERE TRY_STRPTIME(fna_date, '%-m/%-d/%y') IS NOT NULL) AS m_d_yy,
          COUNT(*) FILTER (WHERE COALESCE(
              TRY_CAST(fna_date AS DATE),
              TRY_STRPTIME(fna_date, '%-m/%-d/%Y')::DATE,
              TRY_STRPTIME(fna_date, '%m/%d/%Y')::DATE,
              TRY_STRPTIME(fna_date, '%-m/%-d/%y')::DATE,
              TRY_STRPTIME(fna_date, '%m/%d/%y')::DATE
            ) IS NOT NULL) AS combined
        FROM fna_cytology
    """).fetchone()
    print(f"  total={parsed_stats[0]} iso={parsed_stats[1]} "
          f"m_d_yyyy={parsed_stats[2]} m_d_yy={parsed_stats[3]} "
          f"combined={parsed_stats[4]}")

    # 6. Check if research_id types match across sources
    print("\n--- research_id type variance ---")
    rid_types = con.execute("""
        SELECT table_name, data_type
        FROM information_schema.columns
        WHERE table_schema='main' AND column_name='research_id'
          AND table_name IN ('canonical_patient_master','molecular_test_episode_v2',
                             'thyroseq_molecular_enrichment','extracted_braf_recovery_v1',
                             'ret_patient_adjudicated_v226','fna_cytology',
                             'fna_episode_master_v2','specimen_tumor_focus_v1',
                             'imaging_fna_linkage_v3')
        ORDER BY table_name
    """).fetchall()
    for tn, dt in rid_types:
        print(f"  {tn}: {dt}")

    # 7. Workspace views list — for view compile check
    print("\n--- manuscript_workspace views ---")
    views = con.execute(f"""
        SELECT table_name FROM information_schema.views
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
        ORDER BY table_name
    """).fetchall()
    print(f"  count: {len(views)}")
    # Quick scan: which views reference one of the legacy cols?
    legacy = ['molecular_tested_v7', 'mol_test_count',
              'molecular_platforms_v7', 'n_molecular_tests_v7']
    for vn, in views:
        try:
            sql = con.execute(f"""
                SELECT view_definition FROM information_schema.views
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
                  AND table_name='{vn}'
            """).fetchone()
            if sql and sql[0]:
                hits = [c for c in legacy if c in sql[0]]
                if hits:
                    print(f"  WARN: {vn} references {hits}")
        except Exception as e:
            print(f"  ERR getting def for {vn}: {e}")

    # 8. extracted_fna_bethesda_v1 distribution for delta comparison
    print("\n--- extracted_fna_bethesda_v1 sample ---")
    cols = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='extracted_fna_bethesda_v1'
        ORDER BY ordinal_position
    """).fetchall()
    print(f"  cols: {[c[0] for c in cols]}")

    # 9. Sample link: imaging_fna_linkage_v3 → which is index nodule?
    print("\n--- imaging_nodule_master_v1 ---")
    try:
        cols = con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='imaging_nodule_master_v1'
            ORDER BY ordinal_position
        """).fetchall()
        print(f"  cols: {[c[0] for c in cols]}")
    except Exception as e:
        print(f"  ERR: {e}")

    # 10. extracted_braf_recovery_v1 — distinct rows per patient?
    print("\n--- extracted_braf_recovery_v1 patient distribution ---")
    bdist = con.execute("""
        SELECT n_rows, COUNT(*) FROM (
          SELECT research_id, COUNT(*) AS n_rows
          FROM extracted_braf_recovery_v1
          WHERE UPPER(braf_status)='POSITIVE'
            AND UPPER(detection_method) LIKE '%NGS%'
          GROUP BY research_id
        )
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    print(f"  rows-per-pt distribution (NGS-positive): {bdist}")

    # 11. Check what 'first_surgery_date' column type is
    print("\n--- first_surgery_date type on CPM ---")
    fsd_type = con.execute("""
        SELECT data_type FROM information_schema.columns
        WHERE table_schema='main' AND table_name='canonical_patient_master'
          AND column_name='first_surgery_date'
    """).fetchone()
    print(f"  type: {fsd_type}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
