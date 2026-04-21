"""
Repoint the orphan view manuscript_workspace.tirads_llm_haiku_vs_qwen_v1 from
the archived main.tirads_v2_nodule_patient_rollup_v1 to CPM.

All three "qwen" columns the view used are mirrored on CPM:
  - tirads_v2_n_nodules_scored  (already in CPM pre-Prompt 6)
  - tirads_v2_worst_category    (already in CPM pre-Prompt 6)
  - tirads_v2_worst_rank        (backfilled by Script 348)

Haiku side (tirads_llm_extracted_v2) is unchanged — that table is still in
main (deferred until RunPod tirads_granular re-extraction lands).

Logs to prompt6_view_rebuild_log_v1.
"""

from scripts._md_connect import connect_locked
con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'

new_sql = f"""
CREATE VIEW {DB}.manuscript_workspace.tirads_llm_haiku_vs_qwen_v1 AS
WITH haiku AS (
  SELECT CAST(research_id AS VARCHAR) AS research_id,
         count_star() FILTER (WHERE tirads_level_2017 IS NOT NULL)
           AS n_nodules_scored_haiku,
         max(CASE
               WHEN tirads_level_2017 = 'TR1' THEN 1
               WHEN tirads_level_2017 = 'TR2' THEN 2
               WHEN tirads_level_2017 = 'TR3' THEN 3
               WHEN tirads_level_2017 = 'TR4' THEN 4
               WHEN tirads_level_2017 = 'TR5' THEN 5
               ELSE NULL END) AS haiku_worst_rank,
         max(tirads_level_2017) AS haiku_worst_category
    FROM {DB}.main.tirads_llm_extracted_v2
   GROUP BY 1
),
qwen AS (
  SELECT CAST(research_id AS VARCHAR)        AS research_id,
         tirads_v2_n_nodules_scored          AS n_nodules_scored_qwen,
         tirads_v2_worst_rank                AS qwen_worst_rank,
         tirads_v2_worst_category            AS qwen_worst_category
    FROM {DB}.main.canonical_patient_master
   WHERE tirads_v2_n_nodules_scored IS NOT NULL
      OR tirads_v2_worst_rank IS NOT NULL
      OR tirads_v2_worst_category IS NOT NULL
)
SELECT COALESCE(h.research_id, q.research_id) AS research_id,
       h.n_nodules_scored_haiku,
       h.haiku_worst_category,
       h.haiku_worst_rank,
       q.n_nodules_scored_qwen,
       q.qwen_worst_category,
       q.qwen_worst_rank,
       CASE
         WHEN h.research_id IS NULL THEN 'qwen_only'
         WHEN q.research_id IS NULL THEN 'haiku_only'
         WHEN h.haiku_worst_rank IS NULL OR q.qwen_worst_rank IS NULL
              THEN 'one_run_unscored'
         WHEN h.haiku_worst_category = q.qwen_worst_category THEN 'agree'
         ELSE 'disagree'
       END AS concordance_class,
       COALESCE(q.qwen_worst_rank, 0) - COALESCE(h.haiku_worst_rank, 0)
         AS qwen_minus_haiku_rank
  FROM haiku AS h
  FULL JOIN qwen AS q USING (research_id)
"""

con.execute(
    f'DROP VIEW IF EXISTS {DB}.manuscript_workspace.tirads_llm_haiku_vs_qwen_v1'
)
con.execute(new_sql)

# Smoke test
n = con.execute(
    f"SELECT COUNT(*) FROM {DB}.manuscript_workspace.tirads_llm_haiku_vs_qwen_v1"
).fetchone()[0]
print(f"  view returns {n} rows after repoint")

dist = con.execute(f"""
    SELECT concordance_class, COUNT(*) FROM {DB}.manuscript_workspace.tirads_llm_haiku_vs_qwen_v1
     GROUP BY 1 ORDER BY 2 DESC
""").fetchall()
print(f"  concordance distribution: {dist}")

con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_view_rebuild_log_v1
    VALUES (?, ?, ?, ?, ?, NOW())
""", [353, "manuscript_workspace.tirads_llm_haiku_vs_qwen_v1",
      "main.tirads_v2_nodule_patient_rollup_v1 (archived in 348)",
      "main.canonical_patient_master (tirads_v2_* columns; backfilled in 348)",
      "repointed_to_cpm_after_348_archive"])

print("DONE.")
