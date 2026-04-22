#!/usr/bin/env python3
"""Step 0 — CPM invariant pre-check."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

con = connect_locked()
r = con.execute("""
  SELECT COUNT(*), COUNT(DISTINCT research_id),
         SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
  FROM main.canonical_patient_master
""").fetchone()
print(f"DB: {PUBLICATION_DB}")
print(f"CPM invariant: {r}")
assert r == (10871, 10871, 0), f"CPM invariant FAIL: {r}"
print("PASS: CPM invariant (10871, 10871, 0)")
