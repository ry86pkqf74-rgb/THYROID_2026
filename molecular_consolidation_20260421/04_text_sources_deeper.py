"""Deeper inspection of where ThyroSeq/Afirma report text actually lives."""
from __future__ import annotations

import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, REPO_ROOT)

import duckdb
from motherduck_client import get_token  # type: ignore


def main() -> None:
    os.environ["motherduck_token"] = get_token() or ""
    con = duckdb.connect("md:thyroid_canonical_publication_v1_0")

    print("--- thyroseq_molecular_enrichment text-ish columns ---")
    df = con.execute(
        """
        SELECT
            COUNT(*) AS n_total,
            AVG(length(mutation_raw))::INTEGER  AS avg_mutation_len,
            AVG(length(fusion_raw))::INTEGER    AS avg_fusion_len,
            AVG(length(gep_raw))::INTEGER       AS avg_gep_len,
            AVG(length(pathology_raw))::INTEGER AS avg_pathology_len,
            SUM(CASE WHEN length(pathology_raw) > 200 THEN 1 ELSE 0 END) AS n_path_long,
            SUM(CASE WHEN upper(pathology_raw) LIKE '%DETAILED RESULTS%' THEN 1 ELSE 0 END) AS n_path_with_block
        FROM thyroseq_molecular_enrichment
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- thyroseq_molecular_enrichment counts by molecular_platform ---")
    df = con.execute(
        """
        SELECT molecular_platform, COUNT(*) AS n,
               SUM(CASE WHEN upper(pathology_raw) LIKE '%DETAILED RESULTS%' THEN 1 ELSE 0 END) AS n_with_block,
               SUM(CASE WHEN length(pathology_raw) > 200 THEN 1 ELSE 0 END) AS n_path_long,
               SUM(CASE WHEN length(mutation_raw) > 0 THEN 1 ELSE 0 END) AS n_mut,
               SUM(CASE WHEN length(fusion_raw) > 0 THEN 1 ELSE 0 END) AS n_fus,
               SUM(CASE WHEN length(gep_raw) > 0 THEN 1 ELSE 0 END) AS n_gep
        FROM thyroseq_molecular_enrichment
        GROUP BY molecular_platform
        ORDER BY n DESC
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- sample thyroseq_molecular_enrichment.pathology_raw containing DETAILED RESULTS (3 rows, head 800) ---")
    df = con.execute(
        """
        SELECT research_id, molecular_platform,
               substr(pathology_raw, 1, 800) AS path_head,
               length(pathology_raw) AS path_len
        FROM thyroseq_molecular_enrichment
        WHERE upper(pathology_raw) LIKE '%DETAILED RESULTS%'
        USING SAMPLE 3 ROWS
        """
    ).fetchdf()
    for _, r in df.iterrows():
        print(f"\nresearch_id={r['research_id']} platform={r['molecular_platform']} len={r['path_len']}")
        print(r["path_head"])

    print("\n--- molecular_testing.detailed_findings sample (3 rows where long) ---")
    df = con.execute(
        """
        SELECT research_id, thyroseq_afirma, genetic_test, result,
               substr(detailed_findings, 1, 600) AS df_head,
               length(detailed_findings) AS df_len
        FROM molecular_testing
        WHERE length(detailed_findings) > 200
        USING SAMPLE 3 ROWS
        """
    ).fetchdf()
    for _, r in df.iterrows():
        print(f"\nresearch_id={r['research_id']} sys={r['thyroseq_afirma']} test={r['genetic_test']} result={r['result']} len={r['df_len']}")
        print(r["df_head"])

    print("\n--- molecular_results.raw_payload_json: structure peek ---")
    df = con.execute(
        """
        SELECT
            COUNT(*) AS n_total,
            SUM(CASE WHEN raw_payload_json IS NOT NULL THEN 1 ELSE 0 END) AS n_json,
            AVG(length(raw_payload_json::VARCHAR))::INTEGER AS avg_json_len,
            SUM(CASE WHEN length(raw_payload_json::VARCHAR) > 500 THEN 1 ELSE 0 END) AS n_long_json
        FROM molecular_results
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- molecular_results.raw_payload_json sample top-level keys ---")
    df = con.execute(
        """
        SELECT json_keys(raw_payload_json) AS keys, COUNT(*) AS n
        FROM molecular_results
        WHERE raw_payload_json IS NOT NULL
        GROUP BY 1
        ORDER BY n DESC
        LIMIT 10
        """
    ).fetchdf()
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
