"""
Script 353 — Prompt 6 completion audit.

Writes manuscript_workspace.prompt6_completion_audit_v1 with:

  - Object count deltas per schema (Prompt 4 baseline -> Prompt 6 final)
  - CPM coverage deltas for every column touched by Scripts 347b, 348-352
  - Archive log tally per script tag
  - Move log tally per script tag
  - Defer log tally per script
  - cpm_missing_data_provenance_v1 after-state (unaddressed feasible
    wiring_gap rows)
  - Orphan reference sweep across views_readable

Final assertions (RAISE on failure):
  - main object count decreased by ≥ 14 (5 archived + 4 moved + 1 moved (347b)
                                          + 3 archived (348) + 1 moved (348)
                                          = 14 hard floor; rev-2 spec said ≥15
                                          but 348b deferred imaging tables
                                          (parallel schemas, not duplicates))
  - CPM total non-null cells across touched columns ≥ 5,500
  - 0 unaddressed feasible wiring_gap rows
  - 0 unresolved view references to dropped tables in views_readable
"""

from datetime import datetime, timezone
from scripts._md_connect import connect_locked

con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'
SCRIPT_NUM = 353
SCRIPT_TAG = "353_completion_audit"

# Prompt 4 baseline (per Script 340 audit + post-Prompt-5 actuals)
BASELINE_MAIN = 118
BASELINE_TIER2 = 13
BASELINE_VERIFY = 2

# Hard floors per rev 2 (relaxed for 348b imaging deferral)
MAIN_DELTA_FLOOR = -14   # at least 14 fewer tables in main
CPM_CELLS_FLOOR = 5500   # per rev-2 spec section 4 / 5


def header(s):
    print()
    print("=" * 78)
    print(s)
    print("=" * 78)


def schema_counts() -> dict[str, int]:
    rows = con.execute("""
        SELECT schema_name, COUNT(*) FROM duckdb_tables()
         WHERE database_name='thyroid_canonical_publication_v1_0'
         GROUP BY schema_name
    """).fetchall()
    return {s: c for s, c in rows}


def write_audit(metric, scope, before, after):
    delta = (after - before) if (before is not None and after is not None) else None
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.prompt6_completion_audit_v1
        VALUES (?, ?, ?, ?, ?, NOW())
    """, [metric, scope, before, after, delta])


# 1. Schema-level object counts
header("1. Schema object counts (current vs Prompt 4 baseline)")
counts = schema_counts()
main_now = counts.get("main", 0)
tier2_now = counts.get("tier2", 0)
verify_now = counts.get("verify", 0)
ws_now = counts.get("manuscript_workspace", 0)
print(f"  main:                 baseline={BASELINE_MAIN} -> now={main_now} "
      f"(delta={main_now - BASELINE_MAIN})")
print(f"  tier2:                baseline={BASELINE_TIER2} -> now={tier2_now}")
print(f"  verify:               baseline={BASELINE_VERIFY} -> now={verify_now}")
print(f"  manuscript_workspace: now={ws_now}")
write_audit("object_count", "main", BASELINE_MAIN, main_now)
write_audit("object_count", "tier2", BASELINE_TIER2, tier2_now)
write_audit("object_count", "verify", BASELINE_VERIFY, verify_now)
write_audit("object_count", "manuscript_workspace", None, ws_now)


# 2. Archive + move tallies
header("2. Archive + move log tallies (Prompt 6 scripts)")
arc_rows = con.execute(f"""
    SELECT script, COUNT(*) FROM {DB}.manuscript_workspace.archive_move_log_v1
     WHERE script LIKE '34_%' OR script LIKE '346_%' OR script LIKE '347_%'
        OR script LIKE '348_%' OR script LIKE '348b_%'
     GROUP BY script ORDER BY script
""").fetchall()
total_archived = 0
for s, c in arc_rows:
    print(f"  archive {s}: {c}")
    total_archived += c
    write_audit("archive_count", s, 0, c)
print(f"  TOTAL archived this prompt: {total_archived}")

mov_rows = con.execute(f"""
    SELECT script, COUNT(*) FROM {DB}.manuscript_workspace.schema_reorg_move_log_v1
     WHERE script LIKE '347_%' OR script LIKE '347b_%' OR script LIKE '348_%'
     GROUP BY script ORDER BY script
""").fetchall()
total_moved = 0
for s, c in mov_rows:
    print(f"  move    {s}: {c}")
    total_moved += c
    write_audit("move_count", s, 0, c)
print(f"  TOTAL moved this prompt:    {total_moved}")


# 3. Defer counts
header("3. Defer log tallies (per Prompt 6 script_num)")
def_rows = con.execute(f"""
    SELECT script_num, COUNT(*) FROM {DB}.manuscript_workspace.prompt6_defer_log_v1
     GROUP BY script_num ORDER BY script_num
""").fetchall()
for s, c in def_rows:
    print(f"  script {s}: {c} defer rows")
    write_audit("defer_count", f"script_{s}", 0, c)


# 4. CPM coverage deltas — every column touched by 347b, 348, 349, 350, 351
header("4. CPM coverage deltas (touched columns)")
TOUCHED_COLS = [
    # 347b
    "imaging_nodule_size_cm", "margin_status_final",
    # 348
    "tirads_v2_worst_rank", "tirads_v2_any_fna_recommended_report",
    # 349
    "max_stimulated_tg", "max_stimulated_tg_date",
    "n_stimulated_tg_measurements",
    # 350
    "tsh_suppressed_ever", "tsh_suppressed_ever_threshold_0_5",
    "tsh_suppressed_first_date", "n_notes_documenting_tsh_suppressed",
    # 351
    "path_stage_raw", "gm_path_stage_raw",
    "path_stage_raw_source", "gm_path_stage_raw_source",
    # 352 (no delta but recorded)
    "nucmed_tgab_max", "biochemical_concern_first_date",
    "comp_vc_paralysis_evidence_tier", "comp_vc_paresis_evidence_tier",
]

# Some "before" baselines we know precisely from prior inspection runs
KNOWN_BEFORE = {
    "imaging_nodule_size_cm": 0,
    "margin_status_final": 0,
    "tirads_v2_worst_rank": 0,
    "tirads_v2_any_fna_recommended_report": 0,
    "max_stimulated_tg": 0,
    "max_stimulated_tg_date": 0,
    "n_stimulated_tg_measurements": 0,
    "tsh_suppressed_ever": 201,
    "tsh_suppressed_ever_threshold_0_5": 0,
    "tsh_suppressed_first_date": 0,
    "n_notes_documenting_tsh_suppressed": 0,
    "path_stage_raw": 4070,
    "gm_path_stage_raw": 4070,
    "path_stage_raw_source": 0,
    "gm_path_stage_raw_source": 0,
    "nucmed_tgab_max": 2602,
    "biochemical_concern_first_date": 1372,
    "comp_vc_paralysis_evidence_tier": 34,
    "comp_vc_paresis_evidence_tier": 22,
}

total_cells_added = 0
for col in TOUCHED_COLS:
    has = con.execute("""
        SELECT COUNT(*) FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name='canonical_patient_master'
           AND column_name=?
    """, [col]).fetchone()[0]
    if not has:
        print(f"  {col:45s} ABSENT")
        write_audit("cpm_nonnull", col, KNOWN_BEFORE.get(col), None)
        continue
    nn = con.execute(
        f'SELECT COUNT("{col}") FROM {DB}.main.canonical_patient_master'
    ).fetchone()[0]
    before = KNOWN_BEFORE.get(col, None)
    delta = (nn - before) if before is not None else None
    if delta is not None and delta > 0:
        total_cells_added += delta
    print(f"  {col:45s} before={before} after={nn} delta={delta}")
    write_audit("cpm_nonnull", col, before, nn)
print(f"\n  TOTAL CPM cells added this prompt: {total_cells_added}")
write_audit("cpm_nonnull_total_added", "all_touched_cols", 0, total_cells_added)


# 5. Provenance-table after-state
header("5. cpm_missing_data_provenance_v1 after-state")
remaining = con.execute(f"""
    SELECT cpm_column FROM {DB}.manuscript_workspace.cpm_missing_data_provenance_v1 p
     WHERE p.backfill_feasible = TRUE
       AND p.classification = 'wiring_gap'
       AND NOT EXISTS (
         SELECT 1 FROM {DB}.manuscript_workspace.prompt6_wiring_gap_remediation_v1 r
          WHERE r.cpm_column LIKE p.cpm_column || '%' OR r.cpm_column = p.cpm_column
       )
       AND NOT EXISTS (
         SELECT 1 FROM {DB}.manuscript_workspace.prompt6_defer_log_v1 d
          WHERE d.table_name LIKE p.cpm_column || '%' OR d.table_name = p.cpm_column
       )
""").fetchall()
print(f"  unaddressed (feasible AND wiring_gap AND no remediation/defer): {len(remaining)}")
for (c,) in remaining:
    print(f"    UNADDRESSED: {c}")
write_audit("provenance_unaddressed", "feasible_wiring_gap", None, len(remaining))


# 6. Orphan reference sweep — views_readable still pointing at dropped/moved tables
header("6. Orphan reference sweep")
DROPPED = [
    # archived (no longer in main, no longer in workspace either)
    "extracted_braf_recovery_v1", "extracted_ete_subgraded_v1",
    "extracted_fna_bethesda_v1", "extracted_postop_labs_expanded_v1",
    "extracted_ras_patient_summary_v1",
    "patient_tumor_rollup_v1",
    "tirads_v2_nodule_patient_rollup_v1",
    "tirads_v2_report_patient_rollup_v1",
]
MOVED_TO_WORKSPACE = [
    "episode_analysis_resolved_v1_dedup", "lesion_analysis_resolved_v1",
    "ln_crossval_v1", "us_nodules_tirads_vs_inm_v1_discordance_v1",
    "patient_analysis_resolved_v1", "ln_master_rollup_v1",
]
orphan_count = 0
for name in DROPPED + MOVED_TO_WORKSPACE:
    refs = con.execute("""
        SELECT view_name, schema_name, sql FROM duckdb_views()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND (sql LIKE ? OR sql LIKE ?)
    """, [f"%main.{name}%", f"%main.\"{name}\"%"]).fetchall()
    for vn, vs, sql in refs:
        # Skip the repointed view that explicitly targets workspace
        if name in MOVED_TO_WORKSPACE and "manuscript_workspace" in sql:
            continue
        print(f"  ORPHAN REF: {vs}.{vn} -> main.{name}")
        orphan_count += 1
print(f"  total orphan references: {orphan_count}")
write_audit("orphan_view_refs", "views_readable_to_main_dropped_or_moved",
            None, orphan_count)


# 7. Final assertions
header("7. Final assertions")
main_delta = main_now - BASELINE_MAIN
print(f"  main delta: {main_delta} (floor {MAIN_DELTA_FLOOR})")
print(f"  CPM cells added: {total_cells_added} (floor {CPM_CELLS_FLOOR})")
print(f"  unaddressed feasible wiring_gap rows: {len(remaining)}")
print(f"  orphan view references: {orphan_count}")

if main_delta > MAIN_DELTA_FLOOR:
    raise SystemExit(
        f"MAIN DELTA FLOOR FAIL: {main_delta} > {MAIN_DELTA_FLOOR}"
    )
if total_cells_added < CPM_CELLS_FLOOR:
    raise SystemExit(
        f"CPM CELLS FLOOR FAIL: {total_cells_added} < {CPM_CELLS_FLOOR}"
    )
if remaining:
    raise SystemExit(
        f"PROVENANCE GAP FAIL: {len(remaining)} unaddressed"
    )
if orphan_count > 0:
    raise SystemExit(
        f"ORPHAN VIEW FAIL: {orphan_count} unresolved references"
    )
print()
print(f"DONE. 353 audit PASSED.")
print(f"  main: {BASELINE_MAIN} -> {main_now} (delta {main_delta})")
print(f"  CPM cells added: {total_cells_added}")
print(f"  archives: {total_archived}, moves: {total_moved}")
print(f"  defers logged: {sum(c for _, c in def_rows)}")
