#!/usr/bin/env python3
"""
Script 116: Propagate fna_path_outcome to downstream canonical tables

After script 115 classified all patients in patient_refined_master_clinical_v12,
this script adds fna_path_outcome to all downstream tables that need it.

Tables updated:
  patient_analysis_resolved_v1  — patient-level analytic table
  manuscript_cohort_v1          — publication cohort
  analysis_cancer_cohort_v1     — cancer-only analysis subset
  episode_analysis_resolved_v1_dedup — episode-level (partial: 635 orphan episodes stay NULL)
  lesion_analysis_resolved_v1   — lesion-level
  survival_cohort_enriched      — survival analysis cohort

Usage:
  .venv/bin/python scripts/116_propagate_fna_path_outcome.py --md
"""
import argparse
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import toml


def get_token():
    for path in ['motherduck.local.toml', '.streamlit/secrets.toml']:
        try:
            t = toml.load(path)
            for k in ['MD_SA_TOKEN', 'MOTHERDUCK_TOKEN', 'motherduck_token']:
                if k in t and t[k]:
                    return t[k]
        except Exception:
            pass
    for k in ['MD_SA_TOKEN', 'MOTHERDUCK_TOKEN', 'motherduck_token']:
        v = os.environ.get(k)
        if v:
            return v
    raise RuntimeError("No MotherDuck token found")


PATIENT_TABLES = [
    'patient_analysis_resolved_v1',
    'manuscript_cohort_v1',
    'analysis_cancer_cohort_v1',
    'lesion_analysis_resolved_v1',
    'episode_analysis_resolved_v1_dedup',
    'survival_cohort_enriched',
]


def propagate(con, tbl):
    exists = con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_name='{tbl}' AND table_schema='main'"
    ).fetchone()[0]
    if not exists:
        print(f"  {tbl}: NOT FOUND — skipping")
        return

    has_col = con.execute(
        f"SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_name='{tbl}' AND table_schema='main' AND column_name='fna_path_outcome'"
    ).fetchone()[0]

    if not has_col:
        con.execute(f"ALTER TABLE {tbl} ADD COLUMN fna_path_outcome VARCHAR")
        print(f"  {tbl}: added column")

    con.execute(f"""
        UPDATE {tbl} t
        SET fna_path_outcome = pm.fna_path_outcome
        FROM patient_refined_master_clinical_v12 pm
        WHERE CAST(t.research_id AS BIGINT) = pm.research_id
          AND (t.fna_path_outcome IS NULL OR t.fna_path_outcome != pm.fna_path_outcome)
          AND pm.fna_path_outcome IS NOT NULL
    """)

    dist = con.execute(f"""
        SELECT COALESCE(fna_path_outcome, 'NULL') AS v, COUNT(*) AS n
        FROM {tbl} GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    null_n = int(dist[dist['v'] == 'NULL']['n'].sum()) if 'NULL' in dist['v'].values else 0
    total = int(dist['n'].sum())
    note = f" ({null_n} orphan episodes — expected)" if null_n and tbl == 'episode_analysis_resolved_v1_dedup' else ""
    print(f"  {tbl}: OK — total={total}, NULL={null_n}{note}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--md", action="store_true", required=True)
    args = parser.parse_args()

    import duckdb
    token = get_token()
    print(f"Token: SET (len={len(token)})")
    con = duckdb.connect(f"md:Thyroid 2026?motherduck_token={token}")
    con.execute('USE "Thyroid 2026".main')

    print("\nPropagating fna_path_outcome to downstream tables...")
    for tbl in PATIENT_TABLES:
        propagate(con, tbl)

    print("\nFinal summary:")
    for tbl in PATIENT_TABLES:
        try:
            r = con.execute(
                f"SELECT COALESCE(fna_path_outcome,'NULL') AS v, COUNT(*) AS n "
                f"FROM {tbl} GROUP BY 1 ORDER BY 2 DESC"
            ).fetchdf()
            benign = int(r[r['v'] == 'benign']['n'].sum()) if 'benign' in r['v'].values else 0
            malignant = int(r[r['v'] == 'malignant']['n'].sum()) if 'malignant' in r['v'].values else 0
            null_n = int(r[r['v'] == 'NULL']['n'].sum()) if 'NULL' in r['v'].values else 0
            total = int(r['n'].sum())
            print(f"  {tbl}: total={total} | benign={benign} | malignant={malignant} | NULL={null_n}")
        except Exception as e:
            print(f"  {tbl}: ERROR - {e}")

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
