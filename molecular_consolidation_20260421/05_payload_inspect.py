"""Inspect the molecular_raw JSON payload to find report text."""
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

    print("--- molecular_raw payload keys ---")
    df = con.execute(
        """
        SELECT json_keys(json_extract(raw_payload_json, '$.molecular_raw')) AS mr_keys, COUNT(*) AS n
        FROM molecular_results
        WHERE json_extract(raw_payload_json, '$.molecular_raw') IS NOT NULL
        GROUP BY 1 ORDER BY n DESC LIMIT 8
        """
    ).fetchdf()
    print(df.to_string(index=False))

    print("\n--- sample raw_payload_json.molecular_raw (3 rows) ---")
    df = con.execute(
        """
        SELECT research_id, platform,
               substr(json_extract(raw_payload_json, '$.molecular_raw')::VARCHAR, 1, 1800) AS mr_head
        FROM molecular_results
        WHERE platform IN ('ThyroSeq', 'Afirma')
          AND json_extract(raw_payload_json, '$.molecular_raw') IS NOT NULL
        ORDER BY random()
        LIMIT 3
        """
    ).fetchdf()
    for _, r in df.iterrows():
        print(f"\nresearch_id={r['research_id']} platform={r['platform']}")
        print(r["mr_head"])

    print("\n--- counts where mr.detailed_findings is long ---")
    df = con.execute(
        """
        SELECT platform,
               COUNT(*) AS n,
               SUM(CASE WHEN length(json_extract_string(raw_payload_json, '$.molecular_raw.detailed_findings')) > 200 THEN 1 ELSE 0 END) AS n_df_long,
               SUM(CASE WHEN upper(json_extract_string(raw_payload_json, '$.molecular_raw.detailed_findings')) LIKE '%DETAILED RESULTS%' THEN 1 ELSE 0 END) AS n_df_with_block
        FROM molecular_results
        WHERE platform IS NOT NULL
        GROUP BY platform
        ORDER BY n DESC
        """
    ).fetchdf()
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
