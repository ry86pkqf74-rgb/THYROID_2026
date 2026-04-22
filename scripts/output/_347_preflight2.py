#!/usr/bin/env python3
"""Preflight 2 — registry + raw value sampling."""
import sys
sys.path.insert(0, '/Users/ros/THyroid 2026')
from scripts._md_connect import connect_locked

con = connect_locked()

# Registry
print("--- manuscript_workspace.detail_table_registry_v1 columns ---")
for c, t in con.execute(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_schema='manuscript_workspace' AND table_name='detail_table_registry_v1' "
    "ORDER BY ordinal_position").fetchall():
    print(f"  {c:<40s} {t}")

print("\n--- registry rows for lab tables ---")
for r in con.execute("""
    SELECT * FROM manuscript_workspace.detail_table_registry_v1
    WHERE detail_table_name ILIKE '%lab%'
       OR detail_table_name ILIKE '%longitudinal%'
       OR detail_table_name ILIKE '%thyroglobulin%'
""").fetchall():
    print(f"  {r}")

print("\n--- sample 50 raw TSH values (with their parsed numerics) ---")
for r in con.execute("""
    SELECT value_raw, value_numeric, COUNT(*) AS n
    FROM main.longitudinal_lab_canonical_v1
    WHERE lab_name_standardized='tsh'
    GROUP BY 1,2 ORDER BY n DESC LIMIT 50
""").fetchall():
    print(f"  raw={r[0]!r:<35s} num={r[1]!s:<10s} n={r[2]}")

print("\n--- sample 30 PTH raw values ---")
for r in con.execute("""
    SELECT value_raw, value_numeric, COUNT(*) AS n
    FROM main.longitudinal_lab_canonical_v1
    WHERE lab_name_standardized='pth'
    GROUP BY 1,2 ORDER BY n DESC LIMIT 30
""").fetchall():
    print(f"  raw={r[0]!r:<25s} num={r[1]!s:<10s} n={r[2]}")

print("\n--- sample 30 calcium raw values ---")
for r in con.execute("""
    SELECT value_raw, value_numeric, COUNT(*) AS n
    FROM main.longitudinal_lab_canonical_v1
    WHERE lab_name_standardized='calcium'
    GROUP BY 1,2 ORDER BY n DESC LIMIT 30
""").fetchall():
    print(f"  raw={r[0]!r:<25s} num={r[1]!s:<10s} n={r[2]}")

print("\n--- sample 30 vitamin_d raw values ---")
for r in con.execute("""
    SELECT value_raw, value_numeric, COUNT(*) AS n
    FROM main.longitudinal_lab_canonical_v1
    WHERE lab_name_standardized='vitamin_d'
    GROUP BY 1,2 ORDER BY n DESC LIMIT 30
""").fetchall():
    print(f"  raw={r[0]!r:<25s} num={r[1]!s:<10s} n={r[2]}")

print("\n--- sample TgAb raw values containing ':' (titer format) ---")
for r in con.execute("""
    SELECT value_raw, value_numeric, COUNT(*) AS n
    FROM main.longitudinal_lab_canonical_v1
    WHERE lab_name_standardized='anti_thyroglobulin'
      AND (value_raw LIKE '1:%' OR value_raw LIKE '%:%')
    GROUP BY 1,2 ORDER BY n DESC LIMIT 30
""").fetchall():
    print(f"  raw={r[0]!r:<25s} num={r[1]!s:<10s} n={r[2]}")

print("\n--- sample Tg raw values starting with < or > ---")
for r in con.execute("""
    SELECT value_raw, value_numeric, COUNT(*) AS n
    FROM main.longitudinal_lab_canonical_v1
    WHERE lab_name_standardized='thyroglobulin'
      AND (value_raw LIKE '<%' OR value_raw LIKE '>%' OR value_raw ILIKE '%less than%' OR value_raw ILIKE '%greater than%')
    GROUP BY 1,2 ORDER BY n DESC LIMIT 30
""").fetchall():
    print(f"  raw={r[0]!r:<35s} num={r[1]!s:<10s} n={r[2]}")

print("\n--- distribution of value_numeric NULL by analyte ---")
for r in con.execute("""
    SELECT lab_name_standardized,
           SUM(CASE WHEN value_numeric IS NULL THEN 1 ELSE 0 END) AS nulls,
           COUNT(*) AS total
    FROM main.longitudinal_lab_canonical_v1
    GROUP BY 1 ORDER BY 2 DESC
""").fetchall():
    print(f"  {r[0]:<25s} nulls={r[1]:>5} / {r[2]}")

print("\n--- max value_numeric per analyte (post raw) ---")
for r in con.execute("""
    SELECT lab_name_standardized, MIN(value_numeric), MAX(value_numeric)
    FROM main.longitudinal_lab_canonical_v1
    WHERE value_numeric IS NOT NULL
    GROUP BY 1 ORDER BY 1
""").fetchall():
    print(f"  {r[0]:<25s} min={r[1]} max={r[2]}")

print("\n--- archive_pub_v1_0 schema sample ---")
print("DB list:")
for r in con.execute("SELECT database_name FROM duckdb_databases()").fetchall():
    print(f"  {r[0]}")

print("\n--- 113 head ---")
con.close()
