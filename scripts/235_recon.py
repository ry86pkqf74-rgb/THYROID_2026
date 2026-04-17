"""Reconnaissance for Script 235: verify audit findings and DB schemas.

Read-only. No writes. Just print what we find so we can write Script 235 safely.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from motherduck_client import get_token  # noqa: E402

DB = "thyroid_canonical_publication_v1_0"


def banner(txt: str) -> None:
    print("\n" + "=" * 78)
    print(txt)
    print("=" * 78)


def main() -> None:
    token = get_token()
    con = duckdb.connect(f"md:{DB}?motherduck_token={token}")

    banner("0. DATABASE & ROW COUNT")
    print(con.execute("SELECT current_database()").fetchone())
    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id), "
        "COUNT(*) FILTER (WHERE research_id IS NULL), "
        "COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) "
        "FROM canonical_patient_master"
    ).fetchone()
    print(f"canonical_patient_master: rows={row[0]}, distinct_rid={row[1]}, null_rid={row[2]}, null_fna={row[3]}")

    banner("1. CALCIUM CONTAMINATION AUDIT")
    print("\n-- lab_calcium_min distribution --")
    for r in con.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE lab_calcium_min IS NOT NULL) AS n_nonnull,
          COUNT(*) FILTER (WHERE lab_calcium_min > 20) AS gt20,
          COUNT(*) FILTER (WHERE lab_calcium_min > 50) AS gt50,
          COUNT(*) FILTER (WHERE lab_calcium_min > 100) AS gt100,
          ROUND(AVG(lab_calcium_min) FILTER (WHERE lab_calcium_min IS NOT NULL), 2) AS mean_val,
          ROUND(MIN(lab_calcium_min), 3) AS min_val,
          ROUND(MAX(lab_calcium_min), 3) AS max_val
        FROM canonical_patient_master
        """
    ).fetchall():
        print(r)
    print("\n-- postop_calcium_min_value distribution --")
    for r in con.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE postop_calcium_min_value IS NOT NULL) AS n_nonnull,
          COUNT(*) FILTER (WHERE postop_calcium_min_value > 20) AS gt20,
          ROUND(AVG(postop_calcium_min_value) FILTER (WHERE postop_calcium_min_value IS NOT NULL), 2) AS mean_val,
          ROUND(MIN(postop_calcium_min_value), 3) AS min_val,
          ROUND(MAX(postop_calcium_min_value), 3) AS max_val
        FROM canonical_patient_master
        """
    ).fetchall():
        print(r)

    banner("2. extracted_postop_labs_expanded_v1 SCHEMA")
    cols = con.execute(
        """
        SELECT ordinal_position, column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = ? AND table_name = 'extracted_postop_labs_expanded_v1'
          AND table_schema = 'main'
        ORDER BY ordinal_position
        """,
        [DB],
    ).fetchall()
    print(f"total columns: {len(cols)}")
    names = [c[1] for c in cols]
    print(f"unique column names: {len(set(names))}")
    seen: dict[str, int] = {}
    for _, n, _d in cols:
        seen[n] = seen.get(n, 0) + 1
    dupes = {n: c for n, c in seen.items() if c > 1}
    print(f"duplicate column names: {dupes if dupes else 'NONE'}")
    for c in cols[:30]:
        print(f"  {c}")
    if len(cols) > 30:
        print(f"  ... (+{len(cols)-30} more)")

    banner("3. extracted_postop_labs_expanded_v1 CALCIUM CONTAMINATION")
    try:
        n = con.execute(
            "SELECT COUNT(*) FROM extracted_postop_labs_expanded_v1 WHERE lab_type = 'total_calcium'"
        ).fetchone()[0]
        print(f"total_calcium rows: {n}")
        print("\n-- unit distribution for total_calcium --")
        for r in con.execute(
            """
            SELECT unit, COUNT(*) AS n,
                   ROUND(MIN(value), 3) AS min_v,
                   ROUND(MAX(value), 3) AS max_v,
                   ROUND(AVG(value), 3) AS avg_v
            FROM extracted_postop_labs_expanded_v1
            WHERE lab_type = 'total_calcium'
            GROUP BY unit
            ORDER BY n DESC
            """
        ).fetchall():
            print(r)
        print("\n-- contaminated values (>20 OR unit=pg/mL) --")
        for r in con.execute(
            """
            SELECT value, unit, COUNT(*) AS n
            FROM extracted_postop_labs_expanded_v1
            WHERE lab_type = 'total_calcium'
              AND (value > 20 OR unit = 'pg/mL')
            GROUP BY 1, 2
            ORDER BY value DESC
            LIMIT 50
            """
        ).fetchall():
            print(r)
    except Exception as e:
        print(f"ERR: {e}")

    banner("4. has_low_calcium_flag + has_low_pth_flag CURRENT STATE")
    for r in con.execute(
        """
        SELECT 'has_low_calcium_flag' AS col, has_low_calcium_flag::VARCHAR AS val, COUNT(*)
        FROM canonical_patient_master GROUP BY 1, 2
        UNION ALL
        SELECT 'has_low_pth_flag', has_low_pth_flag::VARCHAR, COUNT(*)
        FROM canonical_patient_master GROUP BY 1, 2
        ORDER BY 1, 2
        """
    ).fetchall():
        print(r)

    banner("5. CALCIUM/PTH COLUMNS IN canonical_patient_master")
    for c in con.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = ? AND table_name = 'canonical_patient_master'
          AND (column_name ILIKE '%calcium%' OR column_name ILIKE '%pth%'
               OR column_name ILIKE '%hypo%para%' OR column_name ILIKE '%hypocalc%'
               OR column_name ILIKE '%parathyr%')
        ORDER BY column_name
        """,
        [DB],
    ).fetchall():
        print(c)

    banner("6. longitudinal_lab_canonical_v1 SCHEMA + CALCIUM STATE")
    try:
        cols = con.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_catalog = ? AND table_name = 'longitudinal_lab_canonical_v1'
              AND table_schema = 'main'
            ORDER BY ordinal_position
            """,
            [DB],
        ).fetchall()
        print(f"cols={len(cols)}")
        for c in cols:
            print(f"  {c}")
        print("\n-- analyte_group distribution --")
        for r in con.execute(
            """
            SELECT analyte_group, COUNT(*) AS n
            FROM longitudinal_lab_canonical_v1
            WHERE analyte_group ILIKE '%calc%' OR analyte_group ILIKE '%pth%' OR analyte_group ILIKE '%parath%'
            GROUP BY 1 ORDER BY 2 DESC
            """
        ).fetchall():
            print(r)
    except Exception as e:
        print(f"ERR: {e}")

    banner("7. nsqip_patient_summary SCHEMA + HYPOCALCEMIA STATE")
    try:
        cols = con.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_catalog = ? AND table_name = 'nsqip_patient_summary'
              AND table_schema = 'main'
              AND (column_name ILIKE '%hypocal%' OR column_name ILIKE '%calcium%'
                   OR column_name ILIKE '%pth%' OR column_name ILIKE '%parath%'
                   OR column_name = 'research_id')
            ORDER BY ordinal_position
            """,
            [DB],
        ).fetchall()
        for c in cols:
            print(f"  {c}")
        print("\n-- nsqip_hypocalcemia distribution --")
        for r in con.execute(
            """
            SELECT nsqip_hypocalcemia, COUNT(*) AS n
            FROM nsqip_patient_summary
            GROUP BY 1 ORDER BY 2 DESC
            """
        ).fetchall():
            print(r)
    except Exception as e:
        print(f"ERR: {e}")

    banner("8. NSQIP vs canonical CONCORDANCE (pre-fix)")
    try:
        for r in con.execute(
            """
            SELECT
              n.nsqip_hypocalcemia,
              COUNT(*) AS n,
              COUNT(*) FILTER (WHERE c.comp_hypocalcemia_confirmed = TRUE) AS canon_confirmed,
              ROUND(COUNT(*) FILTER (WHERE c.comp_hypocalcemia_confirmed = TRUE) * 100.0 / NULLIF(COUNT(*),0), 1) AS pct
            FROM nsqip_patient_summary n
            JOIN canonical_patient_master c ON CAST(n.research_id AS VARCHAR) = c.research_id
            WHERE n.nsqip_hypocalcemia IS NOT NULL
            GROUP BY 1
            """
        ).fetchall():
            print(r)
    except Exception as e:
        print(f"ERR: {e}")

    banner("9. complication_phenotype_v1 SCHEMA")
    cols = con.execute(
        """
        SELECT ordinal_position, column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = ? AND table_name = 'complication_phenotype_v1'
          AND table_schema = 'main'
        ORDER BY ordinal_position
        """,
        [DB],
    ).fetchall()
    print(f"total columns: {len(cols)}, unique: {len({c[1] for c in cols})}")
    seen = {}
    for _, n, _d in cols:
        seen[n] = seen.get(n, 0) + 1
    dupes = {n: c for n, c in seen.items() if c > 1}
    print(f"duplicates: {dupes if dupes else 'NONE'}")
    for c in cols:
        print(f"  {c}")

    print("\n-- complication_entity x confirmed_flag pivot --")
    for r in con.execute(
        """
        SELECT complication_entity, confirmed_flag::VARCHAR, COUNT(*) AS n
        FROM complication_phenotype_v1
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
    ).fetchall():
        print(r)

    print("\n-- biochemical_low_ca / biochemical_low_pth state --")
    for r in con.execute(
        """
        SELECT
          COUNT(*) AS total,
          COUNT(*) FILTER (WHERE biochemical_low_ca = TRUE) AS low_ca_true,
          COUNT(*) FILTER (WHERE biochemical_low_ca = FALSE) AS low_ca_false,
          COUNT(*) FILTER (WHERE biochemical_low_ca IS NULL) AS low_ca_null,
          COUNT(*) FILTER (WHERE biochemical_low_pth = TRUE) AS low_pth_true,
          COUNT(*) FILTER (WHERE biochemical_low_pth = FALSE) AS low_pth_false,
          COUNT(*) FILTER (WHERE biochemical_low_pth IS NULL) AS low_pth_null
        FROM complication_phenotype_v1
        """
    ).fetchall():
        print(r)

    banner("10. complication_patient_summary_v1 SCHEMA")
    try:
        cols = con.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_catalog = ? AND table_name = 'complication_patient_summary_v1'
              AND table_schema = 'main'
            ORDER BY ordinal_position
            """,
            [DB],
        ).fetchall()
        print(f"cols={len(cols)}")
        for c in cols:
            print(f"  {c}")
        rc = con.execute("SELECT COUNT(*) FROM complication_patient_summary_v1").fetchone()[0]
        print(f"rows: {rc}")
    except Exception as e:
        print(f"ERR: {e}")

    banner("11. manuscript_workspace views referencing tables we may touch")
    try:
        for r in con.execute(
            """
            SELECT table_name, view_definition
            FROM information_schema.views
            WHERE table_schema = 'manuscript_workspace'
              AND (view_definition ILIKE '%complication_phenotype%'
                   OR view_definition ILIKE '%complication_patient_summary%'
                   OR view_definition ILIKE '%extracted_postop_labs%'
                   OR view_definition ILIKE '%calcium%'
                   OR view_definition ILIKE '%hypocalcemia%')
            """
        ).fetchall():
            print(f"-- view: {r[0]} --")
            print(r[1][:400])
            print()
    except Exception as e:
        print(f"ERR: {e}")

    banner("12. detail_table_registry_v1 STATE")
    try:
        for r in con.execute(
            "SELECT * FROM manuscript_workspace.detail_table_registry_v1"
        ).fetchall():
            print(r)
    except Exception as e:
        print(f"ERR: {e}")

    banner("13. CANDIDATE DEPRECATED/STAGING TABLES")
    try:
        for r in con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_catalog = ? AND table_schema = 'main'
              AND (table_name LIKE '\\_%' ESCAPE '\\'
                   OR table_name ILIKE '%recovery%'
                   OR table_name ILIKE '%adjudication%'
                   OR table_name ILIKE '%rollup%')
            ORDER BY table_name
            """,
            [DB],
        ).fetchall():
            print(r)
    except Exception as e:
        print(f"ERR: {e}")

    banner("14. CURRENT HYPOPARATHYROIDISM STATE")
    for r in con.execute(
        """
        SELECT
          COUNT(*) FILTER (WHERE comp_hypoparathyroidism_confirmed = TRUE) AS confirmed,
          COUNT(*) FILTER (WHERE comp_hypoparathyroidism_transient = TRUE) AS transient,
          COUNT(*) FILTER (WHERE comp_hypoparathyroidism_permanent = TRUE) AS permanent,
          COUNT(*) FILTER (WHERE comp_hypocalcemia_confirmed = TRUE) AS hypocalcemia_confirmed
        FROM canonical_patient_master
        """
    ).fetchall():
        print(r)

    banner("15. TYPE CHECK research_id ACROSS TABLES")
    for r in con.execute(
        """
        SELECT table_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = ?
          AND table_schema IN ('main')
          AND column_name = 'research_id'
          AND table_name IN ('canonical_patient_master', 'extracted_postop_labs_expanded_v1',
                             'longitudinal_lab_canonical_v1', 'nsqip_patient_summary',
                             'complication_phenotype_v1', 'complication_patient_summary_v1')
        ORDER BY table_name
        """,
        [DB],
    ).fetchall():
        print(r)

    banner("RECON COMPLETE")


if __name__ == "__main__":
    main()
