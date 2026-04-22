#!/usr/bin/env python3
"""Pre-flight verification for Script 347 lab consolidation."""
import sys
sys.path.insert(0, '/Users/ros/THyroid 2026')
from scripts._md_connect import connect_locked

con = connect_locked()
print("Connected to thyroid_canonical_publication_v1_0\n")

# 1. CPM invariant
r = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT research_id), "
    "SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END) "
    "FROM main.canonical_patient_master").fetchone()
print(f"CPM invariant: rows={r[0]} dist_rid={r[1]} null_fna={r[2]} "
      f"({'PASS' if (r[0],r[1],r[2])==(10871,10871,0) else 'FAIL'})")

# 2. Pre-state object inventory
print("\n--- PRE-STATE inventory ---")
for name in [
    'longitudinal_lab_canonical_v1',
    'thyroglobulin_lab_canonical_v1',
    'lab_cross_wave_dedup_map_v1',
    'tg_postop_surveillance_windows_v1',
    'tg_timeline_patient_summary_v1',
    'note_entities_llm_labs',
]:
    try:
        r = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.{name}"
        ).fetchone()
        print(f"  main.{name:<48s} rows={r[0]:>7,} pts={r[1]:>5,}")
    except Exception as e:
        print(f"  main.{name:<48s} ERROR: {e}")

# 3. Existing cancer_only views
print("\n--- existing cancer_only / VIEW objects ---")
rows = con.execute("""
    SELECT table_schema, table_name, table_type
    FROM information_schema.tables
    WHERE table_schema IN ('main','views_readable','archive_pub_v1_0')
      AND (table_name ILIKE '%lab%' OR table_name ILIKE '%tg%' OR table_name ILIKE '%longitudinal%')
    ORDER BY table_schema, table_name
""").fetchall()
for s, n, t in rows:
    print(f"  {s}.{n}  ({t})")

# 4. longitudinal_lab_canonical_v1 columns
print("\n--- longitudinal_lab_canonical_v1 columns ---")
cols = con.execute(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_schema='main' AND table_name='longitudinal_lab_canonical_v1' "
    "ORDER BY ordinal_position"
).fetchall()
for c, t in cols:
    print(f"  {c:<40s} {t}")

# 5. thyroglobulin_lab_canonical_v1 columns
print("\n--- thyroglobulin_lab_canonical_v1 columns ---")
cols = con.execute(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_schema='main' AND table_name='thyroglobulin_lab_canonical_v1' "
    "ORDER BY ordinal_position"
).fetchall()
for c, t in cols:
    print(f"  {c:<40s} {t}")

# 6. Per-analyte row counts in longitudinal
print("\n--- longitudinal_lab_canonical_v1 per-analyte counts ---")
for r in con.execute("""
    SELECT lab_name_standardized,
           COUNT(*) AS n_rows,
           COUNT(DISTINCT research_id) AS n_pts
    FROM main.longitudinal_lab_canonical_v1
    GROUP BY 1 ORDER BY n_rows DESC
""").fetchall():
    print(f"  {str(r[0]):<25s} rows={r[1]:>6} pts={r[2]:>5}")

# 7. Source distribution in longitudinal (ingestion_wave + source_table)
print("\n--- longitudinal_lab_canonical_v1: ingestion_wave x source_table x analyte (top 50) ---")
for r in con.execute("""
    SELECT ingestion_wave, source_table, lab_name_standardized, COUNT(*) AS n
    FROM main.longitudinal_lab_canonical_v1
    GROUP BY 1,2,3 ORDER BY n DESC LIMIT 50
""").fetchall():
    print(f"  wave={str(r[0]):<35s} src={str(r[1]):<48s} lab={str(r[2]):<22s} n={r[3]}")

# 8. detail_table_registry_v1 schema
print("\n--- detail_table_registry_v1 columns ---")
cols = con.execute(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_schema='main' AND table_name='detail_table_registry_v1' "
    "ORDER BY ordinal_position"
).fetchall()
for c, t in cols:
    print(f"  {c:<40s} {t}")
print("--- registry rows for lab tables ---")
for r in con.execute("""
    SELECT * FROM main.detail_table_registry_v1
    WHERE detail_table_name ILIKE '%lab%'
       OR detail_table_name ILIKE '%longitudinal%'
       OR detail_table_name ILIKE '%thyroglobulin%'
""").fetchall():
    print(f"  {r}")

# 9. archive_pub_v1_0 schema available?
print("\n--- archive_pub_v1_0 schemas ---")
for r in con.execute(
    "SELECT schema_name FROM information_schema.schemata WHERE schema_name='archive_pub_v1_0'"
).fetchall():
    print(f"  found: {r[0]}")

# 10. Check existing views_readable Labs_*
print("\n--- views_readable.Labs_* ---")
for r in con.execute("""
    SELECT table_name, table_type FROM information_schema.tables
    WHERE table_schema='views_readable' AND table_name ILIKE 'Labs%'
    ORDER BY table_name
""").fetchall():
    print(f"  {r[0]} ({r[1]})")

# 11. unit_standardized distribution
print("\n--- longitudinal_lab_canonical_v1 unit_standardized x analyte ---")
for r in con.execute("""
    SELECT lab_name_standardized, unit_standardized, COUNT(*)
    FROM main.longitudinal_lab_canonical_v1
    GROUP BY 1,2 ORDER BY 1,3 DESC
""").fetchall():
    print(f"  {str(r[0]):<25s} unit={str(r[1]):<15s} n={r[2]}")

# 12. Sample of TSH raw values that need parsing
print("\n--- sample TSH value_raw (non-parsed) ---")
for r in con.execute("""
    SELECT value_raw, value_numeric
    FROM main.longitudinal_lab_canonical_v1
    WHERE lab_name_standardized='tsh'
    ORDER BY value_raw LIMIT 30
""").fetchall():
    print(f"  raw={r[0]!r:<25s} num={r[1]}")

# 13. specimen_collect_dt non-zero hour count in thyroglobulin
print("\n--- thyroglobulin_lab_canonical_v1 timestamp granularity ---")
for r in con.execute("""
    SELECT
        COUNT(*) AS n_rows,
        SUM(CASE WHEN DATE_PART('hour', specimen_collect_dt) <> 0
                  OR DATE_PART('minute', specimen_collect_dt) <> 0 THEN 1 ELSE 0 END) AS n_hhmm,
        COUNT(DISTINCT analyte) AS n_analytes
    FROM main.thyroglobulin_lab_canonical_v1
""").fetchall():
    print(f"  total={r[0]} hhmm_present={r[1]} analytes={r[2]}")
for r in con.execute("""
    SELECT analyte, COUNT(*) FROM main.thyroglobulin_lab_canonical_v1
    GROUP BY 1 ORDER BY 2 DESC
""").fetchall():
    print(f"    {r[0]}: {r[1]}")

# 14. Existing readable views and main views referencing lab tables
print("\n--- existing main views referencing legacy lab tables ---")
for r in con.execute("""
    SELECT table_name, table_type
    FROM information_schema.tables
    WHERE table_schema='main' AND table_type='VIEW'
      AND (table_name ILIKE '%lab%' OR table_name ILIKE '%tg%' OR table_name ILIKE '%thyroglobulin%' OR table_name ILIKE '%longitudinal%')
    ORDER BY table_name
""").fetchall():
    print(f"  {r[0]} ({r[1]})")

con.close()
print("\nPreflight complete.")
