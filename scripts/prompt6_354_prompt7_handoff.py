"""
Script 354 — RunPod readiness re-probe + Prompt 7 handoff.

Read/log only. No destructive operations.

Writes manuscript_workspace.prompt7_handoff_v1 with:

  RunPod target tables (status):
    note_entities_llm_pathology
    note_entities_llm_cervical_ln_detail
    note_entities_llm_tirads_granular
    note_entities_llm_esophageal_invasion (may be absent until Job 3)

  Items deferred from Prompt 6 (every defer-log entry collapsed to a
  prompt7_handoff_v1 row):
    - extracted_tirads_validated_v1 (deferred in 346)
    - tirads_llm_extracted_v2 (deferred in 348)
    - 348b imaging_*_master_v1 duplicates (parallel schemas)
    - margin_status_final inverted-encoding PI review (from 347b)
    - ln_master_rollup_v1 53-col backfill plan (after RunPod cervical_ln_detail)
    - other defer log entries

  Prompt 7 scope summary:
    1. Pathology histology rollup rebuild from new note_entities_llm_pathology
    2. LN summary stats + ln_master_rollup_v1 backfill
    3. TIRADS re-scoring (haiku vs new qwen comparison + canonical refresh)
    4. Esophageal invasion propagation (Script 342 rerun + op_esophageal_inv_any)
    5. PI review queue triage
"""

from datetime import datetime, timezone
from scripts._md_connect import connect_locked

con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'
SCRIPT_NUM = 354


def header(s):
    print()
    print("=" * 78)
    print(s)
    print("=" * 78)


def log(item, status, row_count, last_extracted, notes):
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.prompt7_handoff_v1
        VALUES (?, ?, ?, ?, ?, NOW())
    """, [item, status, row_count, last_extracted, notes])


# 1. RunPod target tables
header("1. RunPod LLM target tables")
RUNPOD_TARGETS = [
    "note_entities_llm_pathology",
    "note_entities_llm_cervical_ln_detail",
    "note_entities_llm_tirads_granular",
    "note_entities_llm_esophageal_invasion",
]
for t in RUNPOD_TARGETS:
    present = con.execute("""
        SELECT COUNT(*) FROM duckdb_tables()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name=?
    """, [t]).fetchone()[0] > 0
    if not present:
        log(f"runpod_target:{t}", "absent_until_job_lands", 0, None,
            "RunPod job has not yet written this table")
        print(f"  {t:50s} ABSENT")
        continue
    n = con.execute(f'SELECT COUNT(*) FROM {DB}.main."{t}"').fetchone()[0]
    rids = con.execute(
        f'SELECT COUNT(DISTINCT research_id) FROM {DB}.main."{t}"'
    ).fetchone()[0]
    cols = {r[0] for r in con.execute("""
        SELECT column_name FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name=?
    """, [t]).fetchall()}
    ts_col = next(
        (c for c in ("extraction_timestamp", "extracted_at",
                     "ingestion_timestamp", "load_timestamp") if c in cols),
        None,
    )
    last_ts = None
    if ts_col:
        try:
            last_ts = con.execute(
                f'SELECT MAX("{ts_col}") FROM {DB}.main."{t}"'
            ).fetchone()[0]
            if isinstance(last_ts, str):
                # extracted_at may be VARCHAR for some llm tables
                pass
        except Exception:
            last_ts = None
    print(f"  {t:50s} rows={n:>6} rids={rids:>5} last_ts={last_ts}")
    log(f"runpod_target:{t}", "present", n,
        last_ts if isinstance(last_ts, datetime) else None,
        f"rids={rids}, ts_col={ts_col}, last_ts_value={last_ts}")


# 2. Deferred items from Prompt 6 (collapsed from defer log)
header("2. Deferred items (from prompt6_defer_log_v1)")
defs = con.execute(f"""
    SELECT script_num, table_name, reason, deferred_to
      FROM {DB}.manuscript_workspace.prompt6_defer_log_v1
     ORDER BY script_num, table_name
""").fetchall()
for sn, tn, reason, deferred_to in defs:
    log(f"defer:script{sn}:{tn}", "deferred", 0, None,
        f"{reason} (target: {deferred_to})")
    print(f"  script {sn}: {tn}")
print(f"  {len(defs)} defer rows propagated to handoff")


# 3. PI review queue items
header("3. PI review queue (open)")
pi = con.execute(f"""
    SELECT script_num, item, default_used, alternative_available, reason
      FROM {DB}.manuscript_workspace.pi_review_queue_v1
     ORDER BY script_num, item
""").fetchall()
for sn, item, default_used, alt, reason in pi:
    log(f"pi_review:script{sn}:{item}", "needs_pi_review", 0, None,
        f"default={default_used} | alternative={alt} | {reason}")
    print(f"  script {sn}: {item} — default={default_used}, alt={alt}")
print(f"  {len(pi)} PI review items")


# 4. Prompt 7 scope summary
header("4. Prompt 7 scope summary")
SCOPE_ROWS = [
    ("scope:p7_pathology_rollup",
     "Rebuild pathology_synoptic / histology rollup from refreshed "
     "note_entities_llm_pathology (RunPod Job 1a)"),
    ("scope:p7_ln_summary",
     "Recompute LN summary stats + ln_master_rollup_v1 53-col CPM backfill "
     "after RunPod cervical_ln_detail re-extraction (Job 1b)"),
    ("scope:p7_tirads_rescore",
     "Re-score TIRADS from refreshed note_entities_llm_tirads_granular; "
     "rebuild tier2.tirads_granular_patient_wide_v1; revisit deferred "
     "tirads_llm_extracted_v2 + extracted_tirads_validated_v1"),
    ("scope:p7_esophageal_propagation",
     "Run note_entities_llm_esophageal_invasion (Job 3); rerun Script 342 "
     "to populate CPM.op_esophageal_inv_any"),
    ("scope:p7_imaging_reconciliation",
     "Decide canonical_us_*_master_v1 vs imaging_*_master_v1 (parallel "
     "schemas with different unique cols — see 348b defer log)"),
    ("scope:p7_pi_review_triage",
     "Walk every pi_review_queue_v1 row; collect PI decisions; close items"),
    ("scope:p7_complication_vc_tiering",
     "Re-run complication tiering for vocal_cord_paralysis / vocal_cord_paresis "
     "(54 + 49 untiered rows in complication_phenotype_v1)"),
    ("scope:p7_recurrence_extraction",
     "New LLM extraction for recurrence_histology, recurrence_site_primary "
     "(canonical_recurrence_v1 currently 0 nonnull on text fields)"),
]
for item, notes in SCOPE_ROWS:
    log(item, "planned_for_prompt7", 0, None, notes)
    print(f"  {item}")


# 5. Final summary
header("5. Final summary")
n_rows = con.execute(
    f"SELECT COUNT(*) FROM {DB}.manuscript_workspace.prompt7_handoff_v1"
).fetchone()[0]
print(f"  prompt7_handoff_v1 total rows: {n_rows}")

# Read-back distribution
dist = con.execute(f"""
    SELECT status, COUNT(*) FROM {DB}.manuscript_workspace.prompt7_handoff_v1
     GROUP BY 1 ORDER BY 2 DESC
""").fetchall()
print("  status distribution:")
for s, c in dist:
    print(f"    {s}: {c}")

print()
print("DONE. Prompt 6 complete. Handoff written for Prompt 7.")
