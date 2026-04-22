#!/usr/bin/env python3
"""Inventory data_dictionary_* tables in thyroid_canonical_publication_v1_0.main."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from _md_connect import connect_locked  # noqa: E402

con = connect_locked()
rows = con.execute("""
  SELECT table_name, (SELECT COUNT(*) FROM information_schema.columns c
                      WHERE c.table_schema=t.table_schema AND c.table_name=t.table_name) AS ncols
  FROM information_schema.tables t
  WHERE table_schema='main' AND table_name LIKE 'data_dictionary%'
  ORDER BY table_name
""").fetchall()
for tn, nc in rows:
    n = con.execute(f'SELECT COUNT(*) FROM main."{tn}"').fetchone()[0]
    print(f"  {tn}: {nc} cols, {n} rows")

# Check v279 has the columns 255 wants
want = ["column_name", "status", "replacement_column_name", "description"]
cols_v279 = [c[0] for c in con.execute("""
  SELECT column_name FROM information_schema.columns
  WHERE table_schema='main' AND table_name='data_dictionary_v279'
  ORDER BY ordinal_position
""").fetchall()]
print(f"\ndata_dictionary_v279 columns: {cols_v279}")
for w in want:
    print(f"  has {w}: {w in cols_v279}")
