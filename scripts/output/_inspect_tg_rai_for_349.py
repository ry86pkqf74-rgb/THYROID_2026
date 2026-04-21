"""Inspect Tg + RAI tables and CPM target columns for Script 349."""
from scripts._md_connect import connect_locked
con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'

for name in ("thyroglobulin_lab_canonical_v1",
             "rai_treatment_episode_v2",
             "longitudinal_lab_canonical_v1"):
    print(f"\n=== {name} ===")
    cols = con.execute("""
        SELECT column_name, data_type FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name=?
         ORDER BY column_index
    """, [name]).fetchall()
    for c, dt in cols:
        print(f"  {c:40s} {dt}")
    n = con.execute(f'SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {DB}.main."{name}"').fetchone()
    print(f"  rows={n[0]}, distinct_rids={n[1]}")

print("\n=== CPM max_stimulated_tg + related cols ===")
cols = con.execute("""
    SELECT column_name, data_type FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name='canonical_patient_master'
       AND (column_name LIKE '%stimulated_tg%' OR column_name LIKE '%tg_%' OR column_name LIKE 'tsh_suppressed%' OR column_name LIKE 'path_stage_raw%' OR column_name LIKE 'gm_path_stage_raw%')
     ORDER BY column_name
""").fetchall()
for c, dt in cols:
    nn = con.execute(f'SELECT COUNT("{c}") FROM {DB}.main.canonical_patient_master').fetchone()[0]
    print(f"  {c:50s} {dt:25s} nonnull={nn}")

# also check first_surgery_date and tumor_1_t/n/m for 350/351
print("\n=== CPM keys for 350/351 ===")
for c in ("first_surgery_date", "tumor_1_t_stage_ajcc8",
          "tumor_1_n_stage_ajcc8", "tumor_1_m_stage_ajcc8",
          "gm_tumor_1_t_stage_ajcc8", "gm_tumor_1_n_stage_ajcc8",
          "gm_tumor_1_m_stage_ajcc8"):
    has = con.execute("""
        SELECT COUNT(*) FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name='canonical_patient_master' AND column_name=?
    """, [c]).fetchone()[0]
    if has:
        nn = con.execute(f'SELECT COUNT("{c}") FROM {DB}.main.canonical_patient_master').fetchone()[0]
        print(f"  CPM.{c:40s} present, nonnull={nn}")
    else:
        print(f"  CPM.{c:40s} ABSENT")
