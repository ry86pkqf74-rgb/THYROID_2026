#!/usr/bin/env python3
"""
THYROID_2026 — Molecular Testing Accurate Count & Standardization
Prompt 4: Pure SQL — NO LLM needed.

Creates: canonical_molecular_tested_v1

Fixes the inflated molecular_tested count (11,923 → actual ~1,000).
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token

OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

DB = "thyroid_ete_fix_20260413"


def connect():
    token = get_token()
    if not token:
        raise RuntimeError("MotherDuck token not found")
    return duckdb.connect(f"md:{DB}?motherduck_token={token}")


MOLECULAR_SQL = """
WITH
-- Source 1: molecular_testing (actual tests performed)
src_mol_testing AS (
    SELECT DISTINCT
        CAST(research_id AS VARCHAR) AS research_id,
        LOWER(TRIM(CAST(genetic_test_performed AS VARCHAR))) AS platform_raw,
        TRY_CAST("date" AS DATE) AS test_date,
        'molecular_testing' AS source_table
    FROM molecular_testing
    WHERE genetic_test_performed IS NOT NULL
      AND TRIM(CAST(genetic_test_performed AS VARCHAR)) NOT IN ('', 'No', 'None', 'no', 'none', 'N/A', 'n/a')
),

-- Source 2: molecular_variant_long (patients with actual variants found)
src_variants AS (
    SELECT DISTINCT
        CAST(research_id AS VARCHAR) AS research_id,
        NULL AS platform_raw,
        NULL AS test_date,
        'molecular_variant_long' AS source_table
    FROM molecular_variant_long
),

-- Source 3: BRAF recovery
src_braf AS (
    SELECT DISTINCT
        CAST(research_id AS VARCHAR) AS research_id,
        NULL AS platform_raw,
        NULL AS test_date,
        'extracted_braf_recovery_v1' AS source_table
    FROM extracted_braf_recovery_v1
),

-- Source 4: RAS summary
src_ras AS (
    SELECT DISTINCT
        CAST(research_id AS VARCHAR) AS research_id,
        NULL AS platform_raw,
        NULL AS test_date,
        'extracted_ras_patient_summary_v1' AS source_table
    FROM extracted_ras_patient_summary_v1
),

-- Source 5: ThyroSeq enrichment (only non-placeholder rows)
src_thyroseq AS (
    SELECT DISTINCT
        CAST(research_id AS VARCHAR) AS research_id,
        'thyroseq' AS platform_raw,
        NULL AS test_date,
        'thyroseq_molecular_enrichment' AS source_table
    FROM thyroseq_molecular_enrichment
    WHERE (mutation_raw IS NOT NULL AND TRIM(CAST(mutation_raw AS VARCHAR)) NOT IN ('', 'x', 'None', 'none', 'N/A'))
       OR (braf_flag = TRUE OR ras_flag = TRUE OR tert_flag = TRUE OR ret_flag = TRUE OR ntrk_flag = TRUE OR alk_flag = TRUE)
),

-- Source 6: note_entities_genetics (NLP-extracted, confirmed positive only)
src_nlp_genetics AS (
    SELECT DISTINCT
        CAST(research_id AS VARCHAR) AS research_id,
        NULL AS platform_raw,
        TRY_CAST(entity_date AS DATE) AS test_date,
        'note_entities_genetics' AS source_table
    FROM note_entities_genetics
    WHERE present_or_negated = 'present'
),

-- Union all sources
all_sources AS (
    SELECT * FROM src_mol_testing
    UNION ALL SELECT * FROM src_variants
    UNION ALL SELECT * FROM src_braf
    UNION ALL SELECT * FROM src_ras
    UNION ALL SELECT * FROM src_thyroseq
    UNION ALL SELECT * FROM src_nlp_genetics
),

-- Per-patient aggregation
per_patient AS (
    SELECT
        research_id,
        COUNT(DISTINCT source_table) AS n_source_tables,
        MIN(test_date) AS first_test_date,
        STRING_AGG(DISTINCT source_table, '|') AS source_tables_joined,
        STRING_AGG(DISTINCT platform_raw, '|') FILTER (WHERE platform_raw IS NOT NULL) AS platforms_raw,
        COUNT(DISTINCT CASE WHEN source_table = 'molecular_testing' THEN platform_raw END) AS n_distinct_tests
    FROM all_sources
    GROUP BY research_id
),

-- Platform classification
with_platforms AS (
    SELECT
        p.*,
        platforms_raw LIKE '%thyroseq%' AS has_thyroseq,
        platforms_raw LIKE '%afirma%' OR platforms_raw LIKE '%affirma%' AS has_afirma,
        platforms_raw LIKE '%quest%' AS has_quest,
        CASE
            WHEN platforms_raw LIKE '%thyroseq%' AND (platforms_raw LIKE '%afirma%' OR platforms_raw LIKE '%affirma%') THEN 'ThyroSeq+Afirma'
            WHEN platforms_raw LIKE '%thyroseq%' THEN 'ThyroSeq'
            WHEN platforms_raw LIKE '%afirma%' OR platforms_raw LIKE '%affirma%' THEN 'Afirma'
            WHEN platforms_raw LIKE '%quest%' THEN 'Quest'
            WHEN platforms_raw IS NOT NULL AND platforms_raw != '' THEN 'Other'
            ELSE 'unknown'
        END AS platform_canonical
    FROM per_patient p
)
SELECT
    wp.research_id,
    TRUE AS molecular_tested_confirmed,
    wp.platform_canonical,
    wp.platforms_raw AS test_platforms_raw,
    GREATEST(wp.n_distinct_tests, 1) AS test_count,
    wp.has_thyroseq,
    wp.has_afirma,
    wp.first_test_date,
    wp.source_tables_joined AS source_tables,
    wp.n_source_tables,
    -- Cross-reference positivity from gold_master
    COALESCE(g.braf_positive_final, FALSE) AS braf_positive_canonical,
    g.braf_variant_raw,
    g.braf_detection_method,
    COALESCE(g.ras_positive_final, FALSE) AS ras_positive_canonical,
    g.ras_subtype_raw,
    COALESCE(g.tert_positive_final, FALSE) AS tert_positive_canonical,
    g.molecular_risk_tier
FROM with_platforms wp
LEFT JOIN gold_master_patient_facts_v1 g
    ON wp.research_id = CAST(g.research_id AS VARCHAR)
"""


def main():
    con = connect()
    print(f"Connected to MotherDuck {DB}")

    # Step 1: Source-level counts
    print("\n=== Step 1: Source-level counts ===")
    sources = {
        "molecular_testing (real tests)": """
            SELECT COUNT(DISTINCT research_id) FROM molecular_testing
            WHERE genetic_test_performed IS NOT NULL
              AND TRIM(CAST(genetic_test_performed AS VARCHAR)) NOT IN ('', 'No', 'None', 'no', 'none', 'N/A')
        """,
        "molecular_variant_long": "SELECT COUNT(DISTINCT research_id) FROM molecular_variant_long",
        "extracted_braf_recovery_v1": "SELECT COUNT(DISTINCT research_id) FROM extracted_braf_recovery_v1",
        "extracted_ras_patient_summary_v1": "SELECT COUNT(DISTINCT research_id) FROM extracted_ras_patient_summary_v1",
        "thyroseq_enrichment (non-placeholder)": """
            SELECT COUNT(DISTINCT research_id) FROM thyroseq_molecular_enrichment
            WHERE (mutation_raw IS NOT NULL AND TRIM(CAST(mutation_raw AS VARCHAR)) NOT IN ('', 'x', 'None', 'none', 'N/A'))
               OR (braf_flag = TRUE OR ras_flag = TRUE OR tert_flag = TRUE OR ret_flag = TRUE OR ntrk_flag = TRUE OR alk_flag = TRUE)
        """,
        "note_entities_genetics (present)": """
            SELECT COUNT(DISTINCT research_id) FROM note_entities_genetics
            WHERE present_or_negated = 'present'
        """,
    }
    for name, sql in sources.items():
        cnt = con.execute(sql).fetchone()[0]
        print(f"  {name}: {cnt} patients")

    # Step 2: Build unified
    print("\n=== Step 2: Building canonical_molecular_tested_v1 ===")
    df = con.execute(MOLECULAR_SQL).fetchdf()
    print(f"  Rows: {len(df)}, Patients: {df['research_id'].nunique()}")

    # Step 3: Summary
    print("\n=== Step 3: Summary ===")
    print(f"  Total patients with confirmed molecular testing: {len(df)}")

    print(f"\n  Platform distribution:")
    pdist = df["platform_canonical"].value_counts()
    for k, v in pdist.items():
        print(f"    {k}: {v} ({100*v/len(df):.1f}%)")

    print(f"\n  Mutation positivity rates (among tested):")
    for col, label in [("braf_positive_canonical", "BRAF"), ("ras_positive_canonical", "RAS"), ("tert_positive_canonical", "TERT")]:
        pos = df[col].sum()
        print(f"    {label}: {int(pos)}/{len(df)} ({100*pos/len(df):.1f}%)")

    print(f"\n  Source table contribution:")
    source_counts = {}
    for _, row in df.iterrows():
        for s in str(row["source_tables"]).split("|"):
            source_counts[s] = source_counts.get(s, 0) + 1
    for k, v in sorted(source_counts.items(), key=lambda x: -x[1]):
        print(f"    {k}: {v}")

    # Compare to old inflated count
    old_count = con.execute("""
        SELECT COUNT(*) FROM gold_master_patient_facts_v1
        WHERE molecular_eligible_flag = TRUE
    """).fetchone()[0]
    print(f"\n  Old molecular_eligible_flag count: {old_count}")
    print(f"  New confirmed molecular tested:    {len(df)}")
    print(f"  Difference (inflation):            {old_count - len(df)}")

    # Save and upload
    out_path = OUTPUT_DIR / "canonical_molecular_tested_v1.parquet"
    df.to_parquet(out_path, index=False)
    print(f"\n  Saved: {out_path}")

    con.execute("CREATE OR REPLACE TABLE canonical_molecular_tested_v1 AS SELECT * FROM read_parquet(?)", [str(out_path)])
    verify = con.execute("SELECT COUNT(*) FROM canonical_molecular_tested_v1").fetchone()[0]
    print(f"  Uploaded to MotherDuck: {verify} rows")

    print("\n✓ Prompt 4 COMPLETE — canonical_molecular_tested_v1 uploaded to MotherDuck")
    con.close()


if __name__ == "__main__":
    main()
