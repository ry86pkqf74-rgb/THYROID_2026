"""Inspect patient_analysis_resolved_v1 to confirm unique columns + view dep + types."""
from scripts._md_connect import connect_locked
con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'

par_cols = con.execute("""
    SELECT column_name, data_type FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name='patient_analysis_resolved_v1'
     ORDER BY column_index
""").fetchall()
cpm_cols = {r[0]: r[1] for r in con.execute("""
    SELECT column_name, data_type FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name='canonical_patient_master'
""").fetchall()}

par_names = {r[0] for r in par_cols}
unique_to_par = par_names - set(cpm_cols)
print(f"patient_analysis_resolved_v1: {len(par_cols)} cols")
print(f"CPM:                          {len(cpm_cols)} cols")
print(f"Unique to par_v1: {len(unique_to_par)}")
for n in sorted(unique_to_par):
    dt = next(r[1] for r in par_cols if r[0] == n)
    nn = con.execute(f"""
        SELECT COUNT(*), COUNT("{n}"), COUNT(DISTINCT "{n}")
          FROM {DB}.main.patient_analysis_resolved_v1
    """).fetchone()
    print(f"  {n:50s} {dt:30s} rows={nn[0]} nonnull={nn[1]} ndistinct={nn[2]}")
    samp = con.execute(f"""
        SELECT "{n}", COUNT(*) FROM {DB}.main.patient_analysis_resolved_v1
         WHERE "{n}" IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 5
    """).fetchall()
    print(f"    sample: {samp}")

print("\nView dependency:")
v = con.execute("""
    SELECT view_name, schema_name, sql FROM duckdb_views()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND (sql LIKE '%patient_analysis_resolved%')
""").fetchall()
for vn, vs, vq in v:
    print(f"  {vs}.{vn}")
    print(f"    sql first 300: {vq[:300]}")
