"""Determine how to link the 1,384 real episodes to the best-available source text."""
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

    print("--- molecular_test_episode_v2.source_table distribution ---")
    df = con.execute(
        """
        SELECT source_table, ingestion_source, COUNT(*) AS n
        FROM molecular_test_episode_v2
        WHERE platform IS NOT NULL AND platform NOT IN ('Other','OTHER','other')
        GROUP BY 1,2 ORDER BY n DESC LIMIT 20
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- per-episode availability of dfr (head) ---")
    df = con.execute(
        """
        SELECT platform,
               COUNT(*) AS n_total,
               SUM(CASE WHEN length(detailed_findings_raw) > 50 THEN 1 ELSE 0 END) AS n_dfr_50,
               SUM(CASE WHEN length(detailed_findings_raw) > 200 THEN 1 ELSE 0 END) AS n_dfr_200,
               SUM(CASE WHEN length(mutation) > 0 THEN 1 ELSE 0 END) AS n_mut,
               SUM(CASE WHEN length(result) > 0 THEN 1 ELSE 0 END) AS n_result
        FROM molecular_test_episode_v2
        WHERE platform IS NOT NULL AND platform NOT IN ('Other','OTHER','other')
        GROUP BY platform
        ORDER BY n_total DESC
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- molecular_results.source_table for ThyroSeq/Afirma platform ---")
    df = con.execute(
        """
        SELECT platform, source_table, COUNT(*) AS n
        FROM molecular_results
        WHERE platform IS NOT NULL
        GROUP BY 1,2 ORDER BY n DESC LIMIT 20
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- molecular_results: how many rows have molecular_episode_id linked ---")
    df = con.execute(
        """
        SELECT
            COUNT(*) AS n_total,
            SUM(CASE WHEN molecular_episode_id IS NOT NULL THEN 1 ELSE 0 END) AS n_linked,
            COUNT(DISTINCT molecular_episode_id) AS n_distinct_eps
        FROM molecular_results
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- thyroseq_molecular_enrichment per research_id distribution ---")
    df = con.execute(
        """
        SELECT n_per_patient, COUNT(*) AS n_patients
        FROM (
            SELECT research_id, COUNT(*) AS n_per_patient
            FROM thyroseq_molecular_enrichment
            GROUP BY research_id
        ) t
        GROUP BY n_per_patient
        ORDER BY n_per_patient
        LIMIT 20
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- coverage of episodes by enrichment / testing via research_id ---")
    df = con.execute(
        """
        WITH eps AS (
            SELECT research_id, molecular_episode_id, platform
            FROM molecular_test_episode_v2
            WHERE platform IS NOT NULL AND platform NOT IN ('Other','OTHER','other')
        )
        SELECT
            COUNT(*) AS n_episodes,
            SUM(CASE WHEN EXISTS (
                SELECT 1 FROM thyroseq_molecular_enrichment e
                WHERE e.research_id = eps.research_id
                  AND length(e.pathology_raw) > 200
            ) THEN 1 ELSE 0 END) AS n_with_enrich_path_long,
            SUM(CASE WHEN EXISTS (
                SELECT 1 FROM thyroseq_molecular_enrichment e
                WHERE e.research_id = eps.research_id
                  AND upper(e.pathology_raw) LIKE '%DETAILED RESULTS%'
            ) THEN 1 ELSE 0 END) AS n_with_enrich_block,
            SUM(CASE WHEN EXISTS (
                SELECT 1 FROM molecular_testing m
                WHERE m.research_id = eps.research_id
                  AND length(m.detailed_findings) > 200
            ) THEN 1 ELSE 0 END) AS n_with_testing_df_long
        FROM eps
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- now check: episodes with ANY usable text from any source ---")
    df = con.execute(
        """
        WITH eps AS (
            SELECT research_id, molecular_episode_id, platform, detailed_findings_raw, mutation, result
            FROM molecular_test_episode_v2
            WHERE platform IS NOT NULL AND platform NOT IN ('Other','OTHER','other')
        ),
        best AS (
            SELECT eps.research_id, eps.molecular_episode_id, eps.platform,
                length(eps.detailed_findings_raw) AS dfr_len,
                (SELECT MAX(length(e.pathology_raw)) FROM thyroseq_molecular_enrichment e
                  WHERE e.research_id = eps.research_id) AS enrich_path_len,
                (SELECT MAX(length(m.detailed_findings)) FROM molecular_testing m
                  WHERE m.research_id = eps.research_id) AS testing_df_len
            FROM eps
        )
        SELECT
            platform,
            COUNT(*) AS n_eps,
            SUM(CASE WHEN COALESCE(dfr_len,0) > 200 OR COALESCE(enrich_path_len,0) > 200 OR COALESCE(testing_df_len,0) > 200 THEN 1 ELSE 0 END) AS n_with_long_text,
            SUM(CASE WHEN COALESCE(dfr_len,0) > 50  OR COALESCE(enrich_path_len,0) > 50  OR COALESCE(testing_df_len,0) > 50  THEN 1 ELSE 0 END) AS n_with_some_text
        FROM best
        GROUP BY platform
        ORDER BY n_eps DESC
        """
    ).fetchdf()
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
