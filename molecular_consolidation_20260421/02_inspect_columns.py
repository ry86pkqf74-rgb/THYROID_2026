"""Inspect content of candidate report-text columns and platform distribution."""
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

    print("--- platform distribution ---")
    df = con.execute(
        """
        SELECT platform, COUNT(*) AS n
        FROM molecular_test_episode_v2
        GROUP BY platform
        ORDER BY n DESC
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- platform_raw distribution (top 20) ---")
    df = con.execute(
        """
        SELECT platform_raw, COUNT(*) AS n
        FROM molecular_test_episode_v2
        GROUP BY platform_raw
        ORDER BY n DESC
        LIMIT 20
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- length stats for candidate text columns ---")
    df = con.execute(
        """
        SELECT
            COUNT(*) AS n_total,
            SUM(CASE WHEN detailed_findings_raw IS NOT NULL AND length(detailed_findings_raw) > 0 THEN 1 ELSE 0 END) AS n_dfr,
            SUM(CASE WHEN result IS NOT NULL AND length(result) > 0 THEN 1 ELSE 0 END) AS n_result,
            SUM(CASE WHEN mutation IS NOT NULL AND length(mutation) > 0 THEN 1 ELSE 0 END) AS n_mutation,
            AVG(length(detailed_findings_raw))::INTEGER AS avg_dfr_len,
            AVG(length(result))::INTEGER AS avg_result_len,
            AVG(length(mutation))::INTEGER AS avg_mutation_len,
            MAX(length(detailed_findings_raw)) AS max_dfr_len
        FROM molecular_test_episode_v2
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- sample detailed_findings_raw (3 rows, first 600 chars), platform != Other ---")
    df = con.execute(
        """
        SELECT molecular_episode_id, platform,
               substr(detailed_findings_raw, 1, 600) AS dfr_head,
               length(detailed_findings_raw) AS dfr_len
        FROM molecular_test_episode_v2
        WHERE platform IS NOT NULL
          AND platform NOT IN ('Other','OTHER','other')
          AND detailed_findings_raw IS NOT NULL
          AND length(detailed_findings_raw) > 200
        USING SAMPLE 3 ROWS
        """
    ).fetchdf()
    for _, r in df.iterrows():
        print(f"\nepisode={r['molecular_episode_id']} platform={r['platform']} len={r['dfr_len']}")
        print(r["dfr_head"])

    print("\n--- count rows where dfr contains DETAILED RESULTS by platform ---")
    df = con.execute(
        """
        SELECT platform,
               COUNT(*) AS n_total,
               SUM(CASE WHEN upper(detailed_findings_raw) LIKE '%DETAILED RESULTS%' THEN 1 ELSE 0 END) AS n_with_block,
               SUM(CASE WHEN detailed_findings_raw IS NOT NULL AND length(detailed_findings_raw) > 100 THEN 1 ELSE 0 END) AS n_with_long_text
        FROM molecular_test_episode_v2
        WHERE platform IS NOT NULL AND platform NOT IN ('Other','OTHER','other')
        GROUP BY platform
        ORDER BY n_total DESC
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- date columns ---")
    df = con.execute(
        """
        SELECT
            COUNT(*) AS n,
            SUM(CASE WHEN test_date_native IS NOT NULL THEN 1 ELSE 0 END) AS n_test_date_native,
            SUM(CASE WHEN resolved_test_date IS NOT NULL THEN 1 ELSE 0 END) AS n_resolved
        FROM molecular_test_episode_v2
        WHERE platform IS NOT NULL AND platform NOT IN ('Other','OTHER','other')
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- note_entities_genetics top entity_type and entity_value_norm ---")
    df = con.execute(
        """
        SELECT entity_type, COUNT(*) AS n
        FROM note_entities_genetics
        GROUP BY entity_type ORDER BY n DESC LIMIT 20
        """
    ).fetchdf()
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
