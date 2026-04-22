from scripts._md_connect import connect_locked
con = connect_locked()

# Get all 23 note_entities_llm_* tables + the 7 older note_entities_* tables
tabs = con.execute("""
    SELECT table_name FROM duckdb_tables()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main'
       AND (table_name LIKE 'note_entities_llm_%' OR table_name LIKE 'note_entities_%')
     ORDER BY table_name
""").fetchall()

# For each, print: has_result_json, timestamp col name, note-id col name, has research_id
print(f"{'table':60s} {'rj':4s} {'ts_col':20s} {'note_id_col':20s} {'rid':4s}")
print("=" * 110)

for (t,) in tabs:
    cols = con.execute(f"""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name='{t}'
    """).fetchall()
    col_names = [c[0] for c in cols]
    has_rj = 'result_json' in col_names
    # Timestamp candidates
    ts_candidates = [c for c in col_names if c in ('extracted_at','extraction_timestamp','ingestion_timestamp','created_at','load_timestamp','inserted_at','extract_ts')]
    # Note id candidates
    nid_candidates = [c for c in col_names if c in ('note_id','note_row_id','note_index','source_note_id','clinical_note_id')]
    has_rid = 'research_id' in col_names
    print(f"{t:60s} {'Y' if has_rj else 'N':4s} {','.join(ts_candidates):20s} {','.join(nid_candidates):20s} {'Y' if has_rid else 'N':4s}")

print()
print("Other relevant columns (note_date, note_type, confidence):")
print(f"{'table':60s} {'note_date':10s} {'note_type':10s}")
for (t,) in tabs:
    cols = con.execute(f"""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name='{t}'
    """).fetchall()
    col_names = [c[0] for c in cols]
    has_nd = 'Y' if 'note_date' in col_names else 'N'
    has_nt = 'Y' if 'note_type' in col_names else 'N'
    print(f"{t:60s} {has_nd:10s} {has_nt:10s}")

# If note_id doesn't exist on llm tables but note_row_id does, document the resolution
print("\n--- Resolution check: can we link note_row_id to clinical_notes_long? ---")
cnl_cols = con.execute("""
    SELECT column_name FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name='clinical_notes_long'
""").fetchall()
print(f"  clinical_notes_long columns: {[c[0] for c in cnl_cols]}")

# Pick one llm table and confirm note_row_id joins correctly
print("\n--- Sample join test: note_entities_llm_frozen_section_detail.note_row_id vs clinical_notes_long ---")
try:
    test = con.execute("""
        SELECT COUNT(*) AS n_match
          FROM "thyroid_canonical_publication_v1_0".main.note_entities_llm_frozen_section_detail a
          JOIN "thyroid_canonical_publication_v1_0".main.clinical_notes_long b
            ON a.note_row_id = b.note_row_id
         WHERE a.result_json IS NOT NULL
         LIMIT 1
    """).fetchone()
    print(f"  join on note_row_id matches: {test[0]}")
except Exception as e:
    print(f"  note_row_id join failed: {e}")
    try:
        test = con.execute("""
            SELECT COUNT(*) AS n_match
              FROM "thyroid_canonical_publication_v1_0".main.note_entities_llm_frozen_section_detail a
              JOIN "thyroid_canonical_publication_v1_0".main.clinical_notes_long b
                ON a.note_id = b.note_id
             LIMIT 1
        """).fetchone()
        print(f"  join on note_id matches: {test[0]}")
    except Exception as e2:
        print(f"  note_id join also failed: {e2}")
