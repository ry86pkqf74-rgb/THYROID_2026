"""One-off exploration for Prompt 5 Scripts 341-345.
Verifies table/column assumptions against MotherDuck before authoring scripts.
"""
import sys, os
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))  # so 'from _md_connect import' works
from _md_connect import connect_locked

con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'

def hr(s):
    print("\n" + "=" * 70 + f"\n{s}\n" + "=" * 70)

hr("[A] clinical_notes_long: shape + OPNOTE counts")
cols = [r[0] for r in con.execute("""
    SELECT column_name FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name='clinical_notes_long'
     ORDER BY column_index
""").fetchall()]
print("  columns:", cols)
r = con.execute(f"""
    SELECT COUNT(*) AS rows, COUNT(DISTINCT research_id) AS rids,
           SUM(CASE WHEN note_type='OPNOTE' THEN 1 ELSE 0 END) AS opnote_rows,
           COUNT(DISTINCT CASE WHEN note_type='OPNOTE' THEN research_id END) AS opnote_rids
      FROM {DB}.main.clinical_notes_long
""").fetchone()
print(f"  total: rows={r[0]} rids={r[1]}")
print(f"  OPNOTE: rows={r[2]} rids={r[3]}")
nt = con.execute(f"SELECT note_type, COUNT(*) FROM {DB}.main.clinical_notes_long GROUP BY 1 ORDER BY 2 DESC").fetchall()
print(f"  note_type rollup: {nt}")

hr("[B] note_entities_operative_detail: entity_type rollup + columns")
cols = [r[0] for r in con.execute("""
    SELECT column_name FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name='note_entities_operative_detail'
     ORDER BY column_index
""").fetchall()]
print("  columns:", cols)
et = con.execute(f"""
    SELECT entity_type, COUNT(*) AS n,
           COUNT(DISTINCT research_id) AS rids,
           COUNT(DISTINCT note_row_id) AS notes
      FROM {DB}.main.note_entities_operative_detail
     GROUP BY 1 ORDER BY 2 DESC
""").fetchall()
for row in et:
    print(f"    {row}")

hr("[C] operative_episode_detail_v2: shape + multi-episode count + columns")
r = con.execute(f"""
    SELECT COUNT(*) AS rows, COUNT(DISTINCT research_id) AS rids
      FROM {DB}.main.operative_episode_detail_v2
""").fetchone()
print(f"  rows={r[0]} rids={r[1]}")
m = con.execute(f"""
    SELECT COUNT(*) FROM (
      SELECT research_id, COUNT(*) AS n FROM {DB}.main.operative_episode_detail_v2 GROUP BY 1
    ) WHERE n >= 2
""").fetchone()
print(f"  patients with >=2 rows: {m[0]}")
nsv2 = con.execute(f"""
    SELECT COUNT(*) FROM {DB}.main.canonical_patient_master WHERE n_surgeries_v2 > 1
""").fetchone()
print(f"  CPM n_surgeries_v2>1: {nsv2[0]}")
oed_cols = [r[0] for r in con.execute("""
    SELECT column_name FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name='operative_episode_detail_v2'
     ORDER BY column_index
""").fetchall()]
print(f"  oed cols ({len(oed_cols)}): {oed_cols}")

hr("[D] note_entities_llm_airway_invasion: shape + esophag mentions")
cols = [r[0] for r in con.execute("""
    SELECT column_name FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name='note_entities_llm_airway_invasion'
     ORDER BY column_index
""").fetchall()]
print("  columns:", cols)
r = con.execute(f"""
    SELECT COUNT(*) AS rows, COUNT(DISTINCT research_id) AS rids,
           COUNT(DISTINCT CASE WHEN result_json IS NOT NULL THEN research_id END) AS rj_rids
      FROM {DB}.main.note_entities_llm_airway_invasion
""").fetchone()
print(f"  rows={r[0]} rids={r[1]} rj_rids={r[2]}")
e = con.execute(f"""
    SELECT COUNT(DISTINCT research_id) FROM {DB}.main.note_entities_llm_airway_invasion
     WHERE LOWER(CAST(result_json AS VARCHAR)) LIKE '%esophag%'
""").fetchone()
print(f"  RIDs with 'esophag' substring: {e[0]}")

hr("[E] note_entities_operative_detail: esophageal_involvement entity rows")
e = con.execute(f"""
    SELECT COUNT(*) AS rows, COUNT(DISTINCT research_id) AS rids,
           SUM(CASE WHEN COALESCE(present_or_negated,'')='present' THEN 1 ELSE 0 END) AS present_rows,
           COUNT(DISTINCT CASE WHEN COALESCE(present_or_negated,'')='present' THEN research_id END) AS present_rids
      FROM {DB}.main.note_entities_operative_detail
     WHERE entity_type='esophageal_involvement'
""").fetchone()
print(f"  esoph_involvement rows={e[0]} rids={e[1]} present_rows={e[2]} present_rids={e[3]}")

hr("[F] CPM op_esophageal_inv_any + sibling + episode flag pre-state")
for col in ['op_esophageal_inv_any', 'op_nlp_esophageal_involvement']:
    try:
        r = con.execute(f"""
            SELECT COUNT(*) AS nn,
                   SUM(CASE WHEN {col} = TRUE THEN 1 ELSE 0 END) AS true_n,
                   SUM(CASE WHEN {col} = FALSE THEN 1 ELSE 0 END) AS false_n
              FROM {DB}.main.canonical_patient_master WHERE {col} IS NOT NULL
        """).fetchone()
        print(f"  CPM.{col}: nonnull={r[0]} TRUE={r[1]} FALSE={r[2]}")
    except Exception as ex:
        print(f"  CPM.{col}: ERR {ex}")
try:
    r = con.execute(f"""
        SELECT COUNT(*) AS nn,
               SUM(CASE WHEN esophageal_involvement_flag = TRUE THEN 1 ELSE 0 END) AS true_n
          FROM {DB}.main.operative_episode_detail_v2 WHERE esophageal_involvement_flag IS NOT NULL
    """).fetchone()
    print(f"  oed_v2.esophageal_involvement_flag: nonnull={r[0]} TRUE={r[1]}")
except Exception as ex:
    print(f"  oed_v2.esophageal_involvement_flag: ERR {ex}")

hr("[G] VC tier columns vs confirmed/suspected booleans")
for entity, tier_col, conf_col, susp_col in [
    ("paralysis", "comp_vc_paralysis_evidence_tier", "comp_vc_paralysis_confirmed", "comp_vc_paralysis_suspected"),
    ("paresis",   "comp_vc_paresis_evidence_tier",   "comp_vc_paresis_confirmed",   "comp_vc_paresis_suspected"),
]:
    try:
        cross = con.execute(f"""
            SELECT
              CASE WHEN {conf_col} = TRUE THEN 'confirmed'
                   WHEN {susp_col} = TRUE THEN 'suspected'
                   ELSE 'neither' END AS status,
              {tier_col} AS tier,
              COUNT(*) AS n
            FROM {DB}.main.canonical_patient_master
           GROUP BY 1, 2
           ORDER BY 1, 2
        """).fetchall()
        print(f"  {entity}: status x tier:")
        for row in cross:
            print(f"    {row}")
    except Exception as ex:
        print(f"  {entity}: ERR {ex}")

hr("[H] Calcium: CPM cols + LLM labs JSON 'calcium' substring count")
for col in ['lab_calcium_first_date','lab_calcium_last_date','lab_calcium_most_recent','lab_calcium_source']:
    try:
        r = con.execute(f"""
            SELECT COUNT(*) FROM {DB}.main.canonical_patient_master WHERE {col} IS NOT NULL
        """).fetchone()
        print(f"  CPM.{col} nonnull = {r[0]}")
    except Exception as ex:
        print(f"  CPM.{col}: ERR {ex}")
try:
    r = con.execute(f"""
        SELECT COUNT(DISTINCT research_id)
          FROM {DB}.main.note_entities_llm_labs
         WHERE LOWER(CAST(result_json AS VARCHAR)) LIKE '%calcium%'
    """).fetchone()
    print(f"  llm_labs RIDs with 'calcium' substring: {r[0]}")
except Exception as ex:
    print(f"  llm_labs: ERR {ex}")

hr("[I] manuscript_workspace existence (for log table creation)")
try:
    r = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_schemas()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='manuscript_workspace'
    """).fetchone()
    print(f"  manuscript_workspace schema exists: {r[0] > 0}")
    if r[0] > 0:
        n = con.execute(f"""
            SELECT COUNT(*) FROM duckdb_tables()
             WHERE database_name='thyroid_canonical_publication_v1_0'
               AND schema_name='manuscript_workspace'
        """).fetchone()
        print(f"  manuscript_workspace tables: {n[0]}")
except Exception as ex:
    print(f"  ERR {ex}")

hr("[J] Archive schema check (for archive moves)")
try:
    n = con.execute("""
        SELECT COUNT(*) FROM duckdb_schemas()
         WHERE database_name='Thyroid 2026 UPdated'
           AND schema_name='archive_pub_v1_0'
    """).fetchone()
    print(f"  Thyroid 2026 UPdated.archive_pub_v1_0 exists: {n[0] > 0}")
except Exception as ex:
    print(f"  ERR {ex}")

print("\nDONE.")
