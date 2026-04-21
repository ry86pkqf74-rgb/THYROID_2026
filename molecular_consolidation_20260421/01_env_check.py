"""Step 1 — Environment check.

Verifies presence of all 13 legacy molecular tables and prints schema /
counts plus a search for the report-text column on
``molecular_test_episode_v2``.
"""
from __future__ import annotations

import os
import sys

# Repo root on sys.path so we can use the project's MotherDuck client / token loader.
HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, REPO_ROOT)

import duckdb
from motherduck_client import get_token  # type: ignore

DB = "thyroid_canonical_publication_v1_0"

LEGACY = [
    "genetics_per_patient_master_v1",
    "molecular_results",
    "thyroseq_molecular_enrichment",
    "molecular_testing",
    "molecular_test_episode_v2",
    "analysis_molecular_subset_v1",
    "genetics_per_test_master_v1",
    "note_entities_genetics",
    "molecular_variant_long",
    "canonical_molecular_tested_v1",
    "molecular_code_crosswalk",
    "molecular_assay_dictionary",
    "molecular_ingestion_runs",
]


def main() -> None:
    token = get_token()
    if not token:
        raise SystemExit("No MotherDuck token resolved. Configure motherduck.local.toml.")
    os.environ["motherduck_token"] = token

    con = duckdb.connect(f"md:{DB}")
    cat, sch = con.execute("SELECT current_catalog(), current_schema()").fetchone()
    print(f"connected: catalog={cat} schema={sch}")

    print("\n--- legacy table presence + row counts ---")
    existing = {
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema()"
        ).fetchall()
    }
    missing = [t for t in LEGACY if t not in existing]
    for t in LEGACY:
        if t in existing:
            n = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
            print(f"  {t:42s} {n:>9,}")
        else:
            print(f"  {t:42s}  MISSING")
    if missing:
        print(f"\nMISSING TABLES: {missing}")

    print("\n--- DESCRIBE molecular_test_episode_v2 ---")
    cols_df = con.execute("DESCRIBE molecular_test_episode_v2").fetchdf()
    print(cols_df.to_string(index=False))

    text_candidates = [
        c
        for c in cols_df["column_name"].tolist()
        if any(
            tok in c.lower()
            for tok in ("text", "report", "det_block", "block", "raw", "body", "content", "result")
        )
    ]
    print(f"\nlikely text columns: {text_candidates}")

    print("\n--- DESCRIBE note_entities_genetics ---")
    notes_cols = con.execute("DESCRIBE note_entities_genetics").fetchdf()
    print(notes_cols.to_string(index=False))


if __name__ == "__main__":
    main()
