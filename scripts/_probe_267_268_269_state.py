#!/usr/bin/env python3
"""Pre-flight probe for scripts 267, 268, 269 - verify live DB state."""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402


def main() -> int:
    con = connect_locked()
    print(f"Connected to {PUBLICATION_DB}")

    # 1. CPM row + col count
    n_cpm = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master").fetchone()[0]
    n_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}'
          AND table_schema='main' AND table_name='canonical_patient_master'
    """).fetchone()[0]
    print(f"CPM: {n_cpm} rows × {n_cols} cols")

    # 2. Check the 4 legacy cols exist
    legacy = ['molecular_tested_v7', 'mol_test_count',
              'molecular_platforms_v7', 'n_molecular_tests_v7']
    rows = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
          AND column_name IN ({','.join(repr(c) for c in legacy)})
        ORDER BY column_name
    """).fetchall()
    print(f"Legacy cols present: {[r[0] for r in rows]}")

    # 3. molecular_test_episode_v2 count + columns
    n_mte = con.execute(
        "SELECT COUNT(*) FROM molecular_test_episode_v2").fetchone()[0]
    print(f"molecular_test_episode_v2 rows: {n_mte}")
    mte_cols = con.execute(f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='molecular_test_episode_v2'
        ORDER BY ordinal_position
    """).fetchall()
    print(f"\nmte_v2 columns ({len(mte_cols)}):")
    for c, t, n in mte_cols:
        print(f"  {c:<40} {t:<25} nullable={n}")

    # 4. fna_cytology schema
    fna_cols = con.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='fna_cytology'
        ORDER BY ordinal_position
    """).fetchall()
    print(f"\nfna_cytology columns ({len(fna_cols)}):")
    for c, t in fna_cols:
        print(f"  {c:<40} {t}")

    # 5. __conventions
    convs = con.execute(f"""
        SELECT * FROM {PUBLICATION_DB}.manuscript_workspace.__conventions
    """).fetchall()
    conv_cols = [c[0] for c in con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
          AND table_name='__conventions'
        ORDER BY ordinal_position
    """).fetchall()]
    print(f"\n__conventions columns: {conv_cols}")
    print(f"__conventions rows ({len(convs)}):")
    for r in convs:
        print(f"  {r}")

    # 6. legacy_column_sweep_v1_1
    has_lcs = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
          AND table_name='legacy_column_sweep_v1_1'
    """).fetchone()[0]
    print(f"\nlegacy_column_sweep_v1_1 exists: {bool(has_lcs)}")
    if has_lcs:
        lcs_cols = con.execute(f"""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}'
              AND table_schema='manuscript_workspace'
              AND table_name='legacy_column_sweep_v1_1'
            ORDER BY ordinal_position
        """).fetchall()
        print(f"  cols: {lcs_cols}")
        n_lcs = con.execute(
            f"SELECT COUNT(*) FROM {PUBLICATION_DB}.manuscript_workspace.legacy_column_sweep_v1_1"
        ).fetchone()[0]
        print(f"  rows: {n_lcs}")

    # 7. canonical_detail_pointer_v1 — check feeders for relevant cols
    pointer_cols = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
          AND table_name='canonical_detail_pointer_v1'
        ORDER BY ordinal_position
    """).fetchall()
    print(f"\ncanonical_detail_pointer_v1 columns: {[c[0] for c in pointer_cols]}")
    pointer_pinned = con.execute(f"""
        SELECT master_column, detail_table_name, schema_name
        FROM {PUBLICATION_DB}.manuscript_workspace.canonical_detail_pointer_v1
        WHERE master_column IN ('mol_n_tests','molecular_tested_confirmed',
                                'mol_has_afirma','mol_has_thyroseq','mol_platform',
                                'bethesda_final')
        ORDER BY master_column
    """).fetchall()
    print(f"Pinned feeders for relevant cols ({len(pointer_pinned)}):")
    for r in pointer_pinned:
        print(f"  {r}")

    # 8. detail_table_registry_v1 has is_authoritative_for_master?
    reg_cols = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
          AND table_name='detail_table_registry_v1'
        ORDER BY ordinal_position
    """).fetchall()
    print(f"\ndetail_table_registry_v1 columns: {[c[0] for c in reg_cols]}")

    # 9. Ghost patient confirmation
    ghost = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master "
        "WHERE TRY_CAST(research_id AS INTEGER) = 7744").fetchone()[0]
    print(f"\nghost rid 7744 in CPM: {ghost} (expect 0)")

    # 10. Check thyroseq_molecular_enrichment / extracted_braf_recovery_v1 / ret_patient_adjudicated_v226
    for t in ('thyroseq_molecular_enrichment',
              'extracted_braf_recovery_v1',
              'ret_patient_adjudicated_v226',
              '_molecular_patient_rollup_v227',
              'canonical_molecular_tested_v1'):
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"\n{t}: {n} rows")
            cols = con.execute(f"""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                  AND table_name='{t}'
                ORDER BY ordinal_position
            """).fetchall()
            for c, ty in cols:
                print(f"    {c:<40} {ty}")
        except Exception as e:
            print(f"\n{t}: ERROR - {e}")

    # 11. Bethesda current state
    print("\n--- bethesda_final current distribution on CPM ---")
    bf = con.execute("""
        SELECT bethesda_final, COUNT(*)
        FROM canonical_patient_master
        GROUP BY bethesda_final ORDER BY 1 NULLS LAST
    """).fetchall()
    for v, n in bf:
        print(f"  {v!r:<10} {n}")

    # 12. fna_cytology method distribution
    print("\n--- fna_cytology.method distribution ---")
    md = con.execute("""
        SELECT method, COUNT(*) FROM fna_cytology
        GROUP BY method ORDER BY 2 DESC
    """).fetchall()
    for m, n in md:
        print(f"  {m!r:<20} {n}")

    # 13. Check workspace views count
    n_views = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.views
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
    """).fetchone()[0]
    print(f"\nmanuscript_workspace views: {n_views}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
