#!/usr/bin/env python3
"""
Script 115: Path Outcome Classification (regex/rules-based)

Classifies fna_path_outcome for patients currently NULL or 'unknown'
using regex against path_synoptics text fields. Zero LLM cost.

Usage:
  .venv/bin/python scripts/115_path_outcome_classification.py --md --audit
  .venv/bin/python scripts/115_path_outcome_classification.py --md --execute
"""
import argparse
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import duckdb
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


def _load_classification_cte():
    sql_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "sql", "path_outcome_classification_v2.sql"
    )
    with open(sql_path) as f:
        return f.read()


CLASSIFICATION_CTE = _load_classification_cte()


def run_audit(con):
    print("=" * 70)
    print("TASK 1: CLASSIFICATION AUDIT (deduped, one row per patient)")
    print("=" * 70)

    # Cross-tab
    r1 = con.execute(CLASSIFICATION_CTE + """
      SELECT current_outcome, regex_classification, COUNT(*) AS patients
      FROM classified GROUP BY 1, 2 ORDER BY 1 NULLS FIRST, 3 DESC
    """).fetchdf()
    print("\n--- Current vs Regex Classification ---")
    print(r1.to_string(index=False))

    # Transitions
    r2 = con.execute(CLASSIFICATION_CTE + """
      SELECT
        COALESCE(current_outcome, 'NULL') || ' -> ' || regex_classification AS transition,
        COUNT(*) AS patients
      FROM classified
      WHERE (current_outcome IS NULL OR current_outcome = 'unknown')
        AND regex_classification IN ('benign', 'malignant', 'borderline_indeterminate')
      GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    print("\n--- Transitions (NULL/unknown -> new classification) ---")
    print(r2.to_string(index=False))

    # Regex totals
    r3 = con.execute(CLASSIFICATION_CTE + """
      SELECT regex_classification, COUNT(*) AS patients
      FROM classified GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    print("\n--- Regex Classification Totals ---")
    print(r3.to_string(index=False))
    print(f"Total: {r3['patients'].sum()}")

    # Current before
    r4 = con.execute("""
      SELECT COALESCE(fna_path_outcome, 'NULL') AS current_outcome, COUNT(*) AS patients
      FROM patient_refined_master_clinical_v12 GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    print("\n--- Current fna_path_outcome (BEFORE) ---")
    print(r4.to_string(index=False))

    return r2, r3


def run_execute(con):
    # ---- TASK 2: CREATE TABLE ----
    print("\n" + "=" * 70)
    print("TASK 2: CREATE TABLE path_outcome_classification_v1")
    print("=" * 70)
    con.execute("DROP TABLE IF EXISTS path_outcome_classification_v1")
    con.execute("CREATE TABLE path_outcome_classification_v1 AS " + CLASSIFICATION_CTE + """
      SELECT
        research_id,
        current_outcome AS current_fna_path_outcome,
        regex_classification,
        'regex_on_path_synoptics_text' AS classification_source,
        bethesda_final,
        bethesda_final_name,
        tumor_1_histologic_type,
        LEFT(all_text, 300) AS all_text_snippet,
        CURRENT_TIMESTAMP AS classified_at
      FROM classified
    """)
    r = con.execute("""
      SELECT regex_classification, COUNT(*) AS patients
      FROM path_outcome_classification_v1 GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    print(r.to_string(index=False))
    print(f"Total rows: {r['patients'].sum()}")

    # ---- TASK 3: BETHESDA CROSS-VALIDATION ----
    print("\n" + "=" * 70)
    print("TASK 3: BETHESDA vs FINAL PATH OUTCOME (cross-validation)")
    print("=" * 70)
    r3 = con.execute("""
      SELECT
        c.bethesda_final_name,
        c.regex_classification AS final_path_outcome,
        COUNT(*) AS patients
      FROM path_outcome_classification_v1 c
      WHERE c.bethesda_final IS NOT NULL
      GROUP BY 1, 2
      ORDER BY 1, 3 DESC
    """).fetchdf()
    print(r3.to_string(index=False))

    # ---- TASK 4: BACKUP + UPDATE ----
    print("\n" + "=" * 70)
    print("TASK 4: BACKUP + UPDATE patient_refined_master_clinical_v12")
    print("=" * 70)

    # 4a: Backup
    con.execute("DROP TABLE IF EXISTS patient_refined_master_clinical_v12_outcome_backup_20260415")
    con.execute("""
      CREATE TABLE patient_refined_master_clinical_v12_outcome_backup_20260415 AS
      SELECT research_id, fna_path_outcome
      FROM patient_refined_master_clinical_v12
    """)
    bk = con.execute("""
      SELECT COUNT(*) AS rows
      FROM patient_refined_master_clinical_v12_outcome_backup_20260415
    """).fetchone()[0]
    print(f"Backup created: {bk} rows")

    # 4b: UPDATE
    con.execute("""
      UPDATE patient_refined_master_clinical_v12 pm
      SET fna_path_outcome = c.regex_classification
      FROM path_outcome_classification_v1 c
      WHERE pm.research_id = c.research_id
        AND (pm.fna_path_outcome IS NULL OR pm.fna_path_outcome = 'unknown')
        AND c.regex_classification IN ('benign', 'malignant', 'borderline_indeterminate')
    """)
    # Count affected
    affected = con.execute("""
      SELECT
        c.regex_classification,
        COUNT(*) AS updated_rows
      FROM patient_refined_master_clinical_v12 pm
      JOIN path_outcome_classification_v1 c ON pm.research_id = c.research_id
      WHERE pm.fna_path_outcome = c.regex_classification
        AND c.current_fna_path_outcome IS DISTINCT FROM c.regex_classification
        AND (c.current_fna_path_outcome IS NULL OR c.current_fna_path_outcome = 'unknown')
        AND c.regex_classification IN ('benign', 'malignant', 'borderline_indeterminate')
      GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    print("\nRows updated by classification:")
    print(affected.to_string(index=False))
    print(f"Total updated: {affected['updated_rows'].sum()}")

    # ---- TASK 5: VERIFICATION ----
    print("\n" + "=" * 70)
    print("TASK 5: VERIFICATION")
    print("=" * 70)

    # 5a: Final distribution
    r5a = con.execute("""
      SELECT fna_path_outcome, COUNT(*) AS patients
      FROM patient_refined_master_clinical_v12
      GROUP BY 1 ORDER BY 2 DESC
    """).fetchdf()
    print("\n--- 5a: Final fna_path_outcome distribution ---")
    print(r5a.to_string(index=False))

    # 5b: Bethesda-to-outcome concordance
    r5b = con.execute("""
      SELECT
        pm.bethesda_final_name,
        pm.fna_path_outcome,
        COUNT(*) AS n
      FROM patient_refined_master_clinical_v12 pm
      WHERE pm.bethesda_final IS NOT NULL AND pm.fna_path_outcome IS NOT NULL
      GROUP BY 1, 2
      ORDER BY 1, 3 DESC
    """).fetchdf()
    print("\n--- 5b: Bethesda-to-Outcome concordance ---")
    print(r5b.to_string(index=False))

    # 5c: Malignancy rate by Bethesda
    r5c = con.execute("""
      SELECT
        bethesda_final_name,
        COUNT(*) AS total,
        COUNT(CASE WHEN fna_path_outcome = 'malignant' THEN 1 END) AS malignant,
        ROUND(100.0 * COUNT(CASE WHEN fna_path_outcome = 'malignant' THEN 1 END) / COUNT(*), 1) AS malignancy_rate_pct
      FROM patient_refined_master_clinical_v12
      WHERE bethesda_final IS NOT NULL
        AND fna_path_outcome IN ('benign', 'malignant', 'borderline_indeterminate')
      GROUP BY 1, bethesda_final
      ORDER BY bethesda_final
    """).fetchdf()
    print("\n--- 5c: Malignancy rate by Bethesda category ---")
    print(r5c.to_string(index=False))

    return r5a, r5b, r5c


def main():
    parser = argparse.ArgumentParser(description="Path outcome classification")
    parser.add_argument("--md", action="store_true", help="Run against MotherDuck")
    parser.add_argument("--audit", action="store_true", help="Audit only (no writes)")
    parser.add_argument("--execute", action="store_true", help="Execute full pipeline")
    args = parser.parse_args()

    if not args.md:
        print("ERROR: --md required (runs against MotherDuck)")
        sys.exit(1)
    if not args.audit and not args.execute:
        print("ERROR: specify --audit or --execute")
        sys.exit(1)

    token = get_token()
    print(f"Token: SET (len={len(token)})")
    con = duckdb.connect(f"md:Thyroid 2026?motherduck_token={token}")
    con.execute('USE "Thyroid 2026".main')

    if args.audit:
        run_audit(con)
    elif args.execute:
        transitions, totals = run_audit(con)
        run_execute(con)

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
