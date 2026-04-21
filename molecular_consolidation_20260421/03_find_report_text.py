"""Locate the source of full ThyroSeq / Afirma report text across legacy tables."""
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

    for tbl in [
        "molecular_testing",
        "thyroseq_molecular_enrichment",
        "molecular_results",
        "canonical_molecular_tested_v1",
    ]:
        print(f"\n========== {tbl} ==========")
        cols = con.execute(f"DESCRIBE {tbl}").fetchdf()
        print(cols.to_string(index=False))

    print("\n--- molecular_testing: candidate text columns sample ---")
    cols = [
        c
        for c in con.execute("DESCRIBE molecular_testing").fetchdf()["column_name"].tolist()
        if any(
            tok in c.lower()
            for tok in ("text", "report", "result", "findings", "interpret", "raw", "mutation",
                        "comment", "note", "block", "body", "detail")
        )
    ]
    print("candidate columns:", cols)
    if cols:
        sel = ", ".join(
            f"AVG(length({c}))::INTEGER AS avg_{c}_len, "
            f"SUM(CASE WHEN {c} IS NOT NULL AND length({c}) > 100 THEN 1 ELSE 0 END) AS n_long_{c}"
            for c in cols
        )
        df = con.execute(f"SELECT COUNT(*) AS n_total, {sel} FROM molecular_testing").fetchdf()
        print(df.to_string(index=False))

    print("\n--- thyroseq_molecular_enrichment: candidate text columns ---")
    cols = [
        c
        for c in con.execute("DESCRIBE thyroseq_molecular_enrichment").fetchdf()["column_name"].tolist()
        if any(
            tok in c.lower()
            for tok in ("text", "report", "result", "findings", "interpret", "raw", "mutation",
                        "comment", "note", "block", "body", "detail")
        )
    ]
    print("candidate columns:", cols)
    if cols:
        sel = ", ".join(
            f"AVG(length({c}))::INTEGER AS avg_{c}_len, "
            f"SUM(CASE WHEN {c} IS NOT NULL AND length({c}) > 100 THEN 1 ELSE 0 END) AS n_long_{c}"
            for c in cols
        )
        df = con.execute(
            f"SELECT COUNT(*) AS n_total, {sel} FROM thyroseq_molecular_enrichment"
        ).fetchdf()
        print(df.to_string(index=False))

    print("\n--- counts of rows containing 'DETAILED RESULTS' across legacy tables ---")
    queries = {
        "molecular_testing.molecular_text": (
            "SELECT COUNT(*) FROM molecular_testing WHERE upper(molecular_text) LIKE '%DETAILED RESULTS%'"
        ),
        "thyroseq_molecular_enrichment.detailed_findings_raw": (
            "SELECT COUNT(*) FROM thyroseq_molecular_enrichment WHERE upper(detailed_findings_raw) LIKE '%DETAILED RESULTS%'"
        ),
    }
    for label, q in queries.items():
        try:
            n = con.execute(q).fetchone()[0]
            print(f"  {label:60s} {n:>5}")
        except Exception as e:
            print(f"  {label:60s} ERR: {e}")


if __name__ == "__main__":
    main()
