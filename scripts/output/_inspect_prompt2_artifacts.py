"""Inspect Prompt 2 artifacts before writing Scripts 337-340."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from _md_connect import connect_locked

con = connect_locked()
DB = "thyroid_canonical_publication_v1_0"

print("=" * 78)
print("1. ALL Prompt-2 artifacts in main")
print("=" * 78)

artifacts = con.execute(f"""
    SELECT table_name FROM duckdb_tables()
     WHERE database_name='{DB}' AND schema_name='main'
       AND (table_name LIKE 'verify_%_v1'
            OR table_name LIKE '%_event_v1'
            OR table_name LIKE '%_patient_wide_v1')
     ORDER BY table_name
""").fetchall()
print(f"Total: {len(artifacts)} tables")
for (t,) in artifacts:
    print(f"  {t}")

print()
print("=" * 78)
print("2. Schema of every verify_*_summary_v1 table")
print("=" * 78)
sum_tabs = [t for (t,) in artifacts if t.endswith("_summary_v1")]
print(f"\n{len(sum_tabs)} summary tables")
all_summary_cols = set()
per_table_summary_cols = {}
for t in sum_tabs:
    cols = con.execute(f"""
        SELECT column_name, data_type FROM duckdb_columns()
         WHERE database_name='{DB}' AND schema_name='main' AND table_name='{t}'
         ORDER BY column_index
    """).fetchall()
    n = con.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
    print(f"\n  {t}  ({n} rows)")
    for (c, dt) in cols:
        print(f"    {c:35s} {dt}")
        all_summary_cols.add(c)
    per_table_summary_cols[t] = [c for (c, _) in cols]

print("\n--- UNION OF ALL SUMMARY COLS ---")
for c in sorted(all_summary_cols):
    in_n = sum(1 for cs in per_table_summary_cols.values() if c in cs)
    print(f"  {c:35s}  in {in_n}/{len(per_table_summary_cols)} tables")

print()
print("=" * 78)
print("3. Schema of every verify_*_v1 (non-summary) detail table")
print("=" * 78)
det_tabs = [t for (t,) in artifacts if t.startswith("verify_") and not t.endswith("_summary_v1")]
print(f"\n{len(det_tabs)} detail tables")
for t in det_tabs:
    cols = con.execute(f"""
        SELECT column_name, data_type FROM duckdb_columns()
         WHERE database_name='{DB}' AND schema_name='main' AND table_name='{t}'
         ORDER BY column_index
    """).fetchall()
    n = con.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
    rid_dist = con.execute(f'SELECT COUNT(DISTINCT research_id) FROM main."{t}"').fetchone()[0] if any(c[0] == 'research_id' for c in cols) else 'N/A'
    print(f"\n  {t}  ({n} rows, {rid_dist} distinct rid)  ({len(cols)} cols)")
    suffix_count = {}
    base_fields = set()
    for (c, dt) in cols:
        for suf in ['_excel', '_llm', '_source_text', '_source_note_ref', '_source_note_date', '_concordance']:
            if c.endswith(suf):
                suffix_count[suf] = suffix_count.get(suf, 0) + 1
                base_fields.add(c[:-len(suf)])
                break
    print(f"    suffix counts: {suffix_count}")
    print(f"    base fields ({len(base_fields)}): {sorted(base_fields)}")
    other = [c for (c, _) in cols if not any(c.endswith(s) for s in ['_excel','_llm','_source_text','_source_note_ref','_source_note_date','_concordance'])]
    print(f"    non-pattern cols: {other}")

print()
print("=" * 78)
print("4. Schema of every *_patient_wide_v1 table")
print("=" * 78)
wide_tabs = [t for (t,) in artifacts if t.endswith("_patient_wide_v1")]
print(f"\n{len(wide_tabs)} wide tables")
for t in wide_tabs:
    cols = con.execute(f"""
        SELECT column_name, data_type FROM duckdb_columns()
         WHERE database_name='{DB}' AND schema_name='main' AND table_name='{t}'
         ORDER BY column_index
    """).fetchall()
    n = con.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
    rid_dist = con.execute(f'SELECT COUNT(DISTINCT research_id) FROM main."{t}"').fetchone()[0] if any(c[0] == 'research_id' for c in cols) else 'N/A'
    print(f"\n  {t}  ({n} rows, {rid_dist} distinct rid, {len(cols)} cols)")
    for (c, dt) in cols[:8]:
        print(f"    {c:45s} {dt}")
    if len(cols) > 8: print(f"    ...({len(cols)-8} more)")

print()
print("=" * 78)
print("5. Schema of every *_event_v1 table")
print("=" * 78)
ev_tabs = [t for (t,) in artifacts if t.endswith("_event_v1")]
print(f"\n{len(ev_tabs)} event tables")
for t in ev_tabs:
    n = con.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
    nc = con.execute(f"""SELECT COUNT(*) FROM duckdb_columns() WHERE database_name='{DB}' AND schema_name='main' AND table_name='{t}'""").fetchone()[0]
    print(f"  {t}: {n} rows, {nc} cols")

print()
print("=" * 78)
print("6. Existing schemas in publication DB")
print("=" * 78)
schemas = con.execute(f"""
    SELECT schema_name FROM duckdb_schemas()
     WHERE database_name='{DB}' ORDER BY schema_name
""").fetchall()
for (s,) in schemas:
    n = con.execute(f"SELECT COUNT(*) FROM duckdb_tables() WHERE database_name='{DB}' AND schema_name='{s}'").fetchone()[0]
    nv = con.execute(f"SELECT COUNT(*) FROM duckdb_views() WHERE database_name='{DB}' AND schema_name='{s}'").fetchone()[0]
    print(f"  {s}: {n} tables, {nv} views")

print()
print("=" * 78)
print("7. Existence of operational tables (logs)")
print("=" * 78)
for (db, sch, tab) in [
    ('thyroid_canonical_publication_v1_0', 'manuscript_workspace', 'archive_move_log_v1'),
    ('thyroid_canonical_publication_v1_0', 'manuscript_workspace', 'schema_reorg_move_log_v1'),
    ('thyroid_canonical_publication_v1_0', 'manuscript_workspace', 'schema_reorg_orphan_references_v1'),
    ('Thyroid 2026 UPdated', 'archive_pub_v1_0', None),
]:
    try:
        if tab:
            r = con.execute(f"""SELECT COUNT(*) FROM duckdb_tables() WHERE database_name='{db}' AND schema_name='{sch}' AND table_name='{tab}'""").fetchone()[0]
            print(f"  {db}.{sch}.{tab}: {'EXISTS' if r else 'MISSING'}")
        else:
            r = con.execute(f'SELECT COUNT(*) FROM duckdb_tables() WHERE database_name=\'{db}\' AND schema_name=\'{sch}\'').fetchone()[0]
            print(f"  {db}.{sch}: {r} archive tables")
    except Exception as e:
        print(f"  {db}.{sch}.{tab}: ERROR {e}")

print()
print("DONE.")
