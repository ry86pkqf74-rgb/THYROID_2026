from scripts._md_connect import connect_locked
con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'

# ============================================================
# AUDIT 1: Current state of main schema — categorize every object
# ============================================================
print("=" * 70)
print("MAIN SCHEMA — current state")
print("=" * 70)

tabs = con.execute(f"""
    SELECT table_name, 'table' AS kind
      FROM duckdb_tables()
     WHERE database_name = 'thyroid_canonical_publication_v1_0'
       AND schema_name = 'main'
     UNION ALL
    SELECT view_name, 'view' FROM duckdb_views()
     WHERE database_name = 'thyroid_canonical_publication_v1_0'
       AND schema_name = 'main'
     ORDER BY 1
""").fetchall()

print(f"\nTotal objects in main: {len(tabs)}")

def cat(name):
    if name == 'canonical_patient_master': return '01_CPM'
    if name.startswith('canonical_'): return '02_canonical_*'
    if name.startswith('note_entities_llm_'): return '03_note_entities_llm_*'
    if name.startswith('note_entities_'): return '04_note_entities_*'
    if name.startswith('path_synoptics') or name.startswith('path_outcome') or name.startswith('path_size'): return '05_path_*'
    if name.startswith('tirads_') or name == 'us_nodules_tirads': return '06_tirads_*'
    if 'ultrasound' in name or name.startswith('us_') or name.startswith('imaging_'): return '07_us/imaging_*'
    if name.startswith('molecular_'): return '08_molecular_*'
    if 'recurrence' in name: return '09_recurrence*'
    if name.startswith('rai_'): return '10_rai_*'
    if name.startswith('operative_') or name.startswith('op_') or 'surgery' in name: return '11_operative_*'
    if 'lab' in name or 'tg_' in name or 'thyroglobulin' in name or 'longitudinal' in name: return '12_labs/tg_*'
    if 'complication' in name or name.startswith('comp_') or name.startswith('nlp_'): return '13_complications/nlp_*'
    if name.startswith('verify_'): return '14_verify_*'
    if '__march2026_broken' in name or '__broken' in name: return '99_BROKEN'
    if name.startswith('__') or name.endswith('__readme'): return '98_meta_*'
    if 'queue' in name or 'adjudication' in name or 'review' in name: return '15_queues_adjudication'
    if 'ln_' in name or 'cervical' in name: return '16_ln_*'
    if 'staging' in name or '_raw' in name or 'extracted_' in name: return '17_raw/extracted_*'
    return '18_other'

from collections import defaultdict
buckets = defaultdict(list)
for (t, k) in tabs:
    buckets[cat(t)].append((t, k))

for c in sorted(buckets.keys()):
    print(f"\n-- {c} ({len(buckets[c])} objects) --")
    for (t, k) in buckets[c]:
        print(f"  [{k:5s}] {t}")

# ============================================================
# AUDIT 2: Verify the 3 allegedly-stale LLM domain extractions
# ============================================================
print("\n" + "=" * 70)
print("STALE DOMAIN CHECK — cervical_ln_detail / pathology / tirads_granular")
print("=" * 70)
for t in ['note_entities_llm_cervical_ln_detail','note_entities_llm_pathology','note_entities_llm_tirads_granular']:
    try:
        r = con.execute(f"""
            SELECT COUNT(*) AS rows, COUNT(DISTINCT research_id) AS rids,
                   MAX(extracted_at) AS max_ts,
                   MIN(extracted_at) AS min_ts
              FROM {DB}.main.\"{t}\"
        """).fetchone()
        print(f"\n  {t}: rows={r[0]}, rids={r[1]}, ts_range={r[2]}..{r[3]}")
        # model tag if present - try simpler approach
        cols = con.execute(f"""
            SELECT column_name FROM duckdb_columns()
             WHERE database_name='thyroid_canonical_publication_v1_0'
               AND schema_name='main' AND table_name='{t}'
               AND (column_name LIKE '%model%' OR column_name LIKE '%extractor%')
        """).fetchall()
        for (c,) in cols:
            dist = con.execute(f'SELECT DISTINCT {c} FROM {DB}.main."{t}" ORDER BY 1').fetchall()
            print(f"    distinct {c}: {[d[0] for d in dist]}")
    except Exception as e:
        print(f"  {t}: ERROR {e}")

# ============================================================
# AUDIT 3: longitudinal_lab_canonical_v1 + calcium coverage
# ============================================================
print("\n" + "=" * 70)
print("LAB COVERAGE — longitudinal_lab_canonical_v1")
print("=" * 70)
try:
    cols = con.execute(f"""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name='longitudinal_lab_canonical_v1'
         ORDER BY column_index
    """).fetchall()
    print(f"  cols: {[c[0] for c in cols]}")
    name_col = 'lab_name_standardized' if any(c[0]=='lab_name_standardized' for c in cols) else ('lab_name_raw' if any(c[0]=='lab_name_raw' for c in cols) else 'analyte')
    print(f"  using name column: {name_col}")
    r = con.execute(f"""
        SELECT {name_col}, COUNT(*) AS n_rows, COUNT(DISTINCT research_id) AS n_rids
          FROM {DB}.main.longitudinal_lab_canonical_v1
         WHERE {name_col} LIKE '%calcium%' OR {name_col} LIKE '%PTH%' OR {name_col} LIKE '%TSH%' OR {name_col} LIKE '%thyroglobulin%' OR {name_col} LIKE '%Tg%'
         GROUP BY 1 ORDER BY 2 DESC LIMIT 30
    """).fetchall()
    print(f"  top lab-name rollup:")
    for row in r:
        print(f"    {row}")
except Exception as e:
    print(f"  ERROR: {e}")

# ============================================================
# AUDIT 4: note_entities_llm_labs.result_json — can we parse calcium?
# ============================================================
print("\n" + "=" * 70)
print("LLM LABS — calcium extractability")
print("=" * 70)
try:
    r = con.execute(f"""
        SELECT COUNT(*) AS rows, COUNT(DISTINCT research_id) AS rids
          FROM {DB}.main.note_entities_llm_labs
         WHERE result_json IS NOT NULL
    """).fetchone()
    print(f"  note_entities_llm_labs: rows={r[0]}, rids={r[1]}")
    # Sample to see entity_types used
    sample = con.execute(f"""
        WITH ent AS (
          SELECT UNNEST(CAST(json_extract(result_json, '$.entities') AS VARCHAR[])) AS e
            FROM {DB}.main.note_entities_llm_labs
           WHERE result_json IS NOT NULL
           USING SAMPLE 2000 ROWS
        )
        SELECT json_extract_string(e, '$.entity_type') AS et, COUNT(*) AS n
          FROM ent GROUP BY 1 ORDER BY 2 DESC LIMIT 30
    """).fetchall()
    print(f"  entity_type rollup (sample of 2000 rows):")
    for row in sample:
        print(f"    {row}")
    # How many rows have calcium-related evidence
    ca = con.execute(f"""
        SELECT COUNT(DISTINCT research_id)
          FROM {DB}.main.note_entities_llm_labs
         WHERE result_json LIKE '%calcium%' OR result_json LIKE '%PTH%' OR result_json LIKE '% Ca %'
    """).fetchone()
    print(f"  distinct RIDs with calcium/PTH evidence in result_json: {ca[0]}")
except Exception as e:
    print(f"  ERROR: {e}")

# ============================================================
# AUDIT 5: op_esophageal_inv_any extractability
# ============================================================
print("\n" + "=" * 70)
print("OP ESOPHAGEAL INV — extractability from existing LLM operative entities")
print("=" * 70)
try:
    for t in ['note_entities_operative_detail','note_entities_procedures','note_entities_llm_airway_invasion']:
        has_rj = con.execute(f"""
            SELECT COUNT(*) FROM duckdb_columns()
             WHERE database_name='thyroid_canonical_publication_v1_0'
               AND schema_name='main' AND table_name='{t}' AND column_name='result_json'
        """).fetchone()[0]
        if has_rj:
            cnt = con.execute(f"""
                SELECT COUNT(DISTINCT research_id) FROM {DB}.main."{t}"
                 WHERE result_json LIKE '%esophag%'
            """).fetchone()[0]
            print(f"  {t} (has result_json): RIDs mentioning 'esophag' = {cnt}")
        else:
            # search other text columns
            cols = con.execute(f"""
                SELECT column_name FROM duckdb_columns()
                 WHERE database_name='thyroid_canonical_publication_v1_0'
                   AND schema_name='main' AND table_name='{t}'
                   AND data_type IN ('VARCHAR','TEXT')
            """).fetchall()
            print(f"  {t}: no result_json. Text cols: {[c[0] for c in cols][:15]}")
except Exception as e:
    print(f"  ERROR: {e}")

# ============================================================
# AUDIT 6: op_sheet_data (for operative re-op rebuild P0)
# ============================================================
print("\n" + "=" * 70)
print("OPERATIVE REBUILD SOURCES")
print("=" * 70)
for t in ['op_sheet_data','note_entities_operative_detail','note_entities_procedures','operative_episode_detail_v1']:
    try:
        r = con.execute(f"""
            SELECT COUNT(*) AS row_count, COUNT(DISTINCT research_id) AS rid_count FROM {DB}.main."{t}"
        """).fetchone()
        print(f"  {t}: rows={r[0]}, rids={r[1]}")
        cols_n = con.execute(f"""
            SELECT COUNT(*) FROM duckdb_columns()
             WHERE database_name='thyroid_canonical_publication_v1_0'
               AND schema_name='main' AND table_name='{t}'
        """).fetchone()[0]
        print(f"    cols: {cols_n}")
        # per-RID row distribution
        dist = con.execute(f"""
            WITH x AS (SELECT research_id, COUNT(*) AS n FROM {DB}.main."{t}" GROUP BY 1)
            SELECT n, COUNT(*) AS n_rids FROM x GROUP BY 1 ORDER BY 1 LIMIT 10
        """).fetchall()
        print(f"    row-per-RID dist (first 10): {dist}")
    except Exception as e:
        print(f"  {t}: NOT FOUND or ERR {e}")

print("\nDONE.")
