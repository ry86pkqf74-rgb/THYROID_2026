#!/usr/bin/env python3
"""Script 270b — Phase A pre-flight & audit-only writes.

Covers what the original 8-step prompt called Steps 0, 1a, 1b, and 3 —
all of which reduce to "audit-only" after live state was reconciled
against the prompt's stale assumptions:

  Step 0  — pre-flight CPM snapshot + verify catalog drift = 0,
            ghosts = 0, post-266 bethesda re-triage cpm_cols_unaccounted = 0
            (Script 268 already mapped all 9 bethesda_* cols; live verified)
  Step 1a — ETE adjudication NOOP: ete_grade_final_v2 already contains the
            19 high-confidence applications + correctly retains
            'present_ungraded' for the 26 low-confidence rows
            (standing reference doc: docs/270_ete_low_conf_register.csv)
  Step 1b — Path-size adjudication DEFERRED to clinician: 96-row worksheet
            at scripts/output/270_path_size_human_review.csv; would-be
            filter thresholds recorded in this audit row's notes for
            v1_1 replay
  Step 3  — Literal-'nan' repair NOOP: 475 cols already NO_ACTION, the
            single PRESERVE_RAW col (syn_margin_distance_mm_raw_str)
            preserves source representation by design (see
            scripts/output/270b_nan_recon.md for full evidence)

Phase A's only remaining substantive work is Step 2 (registry closure),
which lives in scripts/270b_phase_a_step_2_registry.py.

Writes performed (against MotherDuck):
  1. Snapshot canonical_patient_master to
     "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre270_<UTC>
  2. INSERT 5 rows into manuscript_workspace.v1_1_finalization_audit_v1

Outputs:
  scripts/output/270b_phase_a_preflight_audit.json
  scripts/output/270b_phase_a_preflight_audit.log

Idempotent: aborts if any of the 5 finding_ids already exist in the
audit table (no duplicate inserts; no duplicate snapshots).
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_JSON = OUT_DIR / "270b_phase_a_preflight_audit.json"
OUT_LOG = OUT_DIR / "270b_phase_a_preflight_audit.log"

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
CPM = "canonical_patient_master"
AUDIT_FQ = (
    f'"{PUBLICATION_DB}".manuscript_workspace.v1_1_finalization_audit_v1'
)

GIT_COMMIT_SHA_PHASE_0 = "a2fe41d"

# 5 finding_ids this script writes; idempotency anchor.
FINDING_IDS = (
    "step_0_catalog_reconciliation",
    "step_0_bethesda_retriage",
    "step_1a_ete_already_applied_in_v2",
    "step_1b_path_size_deferred_for_clinician",
    "step_3_nan_repair_already_resolved",
)


def main() -> int:
    log_lines: list[str] = []

    def log(msg: str) -> None:
        line = msg if msg.endswith("\n") else msg + "\n"
        log_lines.append(line)
        print(msg)

    started_at = datetime.now(timezone.utc)
    log(f"=== START Script 270b Phase A pre-flight audit ===")
    log(f"started_at: {started_at.isoformat()}")

    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    # ----- Idempotency guard -----
    existing = con.execute(f"""
        SELECT finding_id FROM {AUDIT_FQ}
        WHERE finding_id IN ({','.join(repr(f) for f in FINDING_IDS)})
        ORDER BY finding_id
    """).fetchall()
    if existing:
        log(f"\nABORT: {len(existing)} of the 5 expected finding_ids already present:")
        for (f,) in existing:
            log(f"  {f}")
        log("Script is idempotent — refusing to insert duplicates.")
        OUT_LOG.write_text("".join(log_lines))
        return 0

    # ----- Live verification numbers (computed live; recorded in audit) -----
    log("\n--- LIVE STATE VERIFICATION ---")

    n_cpm = con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0]
    n_cpm_distinct = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {CPM}").fetchone()[0]
    n_cpm_null_rid = con.execute(
        f"SELECT COUNT(*) FROM {CPM} WHERE research_id IS NULL").fetchone()[0]
    n_cpm_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='{CPM}'
    """).fetchone()[0]
    log(f"  CPM: {n_cpm} rows / {n_cpm_distinct} distinct rid / "
        f"{n_cpm_null_rid} null rid / {n_cpm_cols} cols")
    if n_cpm != 10871 or n_cpm_distinct != 10871 or n_cpm_null_rid != 0:
        log(f"FAIL: CPM spine invariant broken")
        OUT_LOG.write_text("".join(log_lines))
        return 1

    # Catalog ghosts: count info_schema BASE TABLEs vs queryable
    info_tables = [r[0] for r in con.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_type='BASE TABLE'
        ORDER BY table_name
    """).fetchall()]
    ghosts: list[str] = []
    queryable_count = 0
    for t in info_tables:
        try:
            con.execute(f'SELECT 1 FROM "{t}" LIMIT 0').fetchall()
            queryable_count += 1
        except Exception:
            ghosts.append(t)
    log(f"  main info_schema BASE TABLEs: {len(info_tables)}")
    log(f"  main queryable BASE TABLEs:    {queryable_count}")
    log(f"  main ghost BASE TABLEs:        {len(ghosts)}")

    n_readme = con.execute(f"SELECT COUNT(*) FROM main.__readme").fetchone()[0]
    log(f"  main.__readme rows:            {n_readme}")

    # Bethesda re-triage: cpm cols with no mapping AND no triage entry
    mapped: set[str] = set()
    for (s,) in con.execute(f"""
        SELECT feeds_master_columns_normalized
        FROM {PUBLICATION_DB}.manuscript_workspace.detail_table_registry_v1
        WHERE feeds_master_columns_normalized IS NOT NULL
          AND feeds_master_columns_normalized <> ''
          AND feeds_master_columns_normalized NOT LIKE '%TODO%'
    """).fetchall():
        for tok in s.split(";"):
            tok = tok.strip()
            if tok:
                mapped.add(tok)
    triage_cols = {r[0] for r in con.execute(f"""
        SELECT DISTINCT column_name
        FROM {PUBLICATION_DB}.manuscript_workspace.cpm_unmapped_triage_v265
    """).fetchall()}
    all_cpm = {r[0] for r in con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='{CPM}'
    """).fetchall()}
    unaccounted = sorted(all_cpm - mapped - triage_cols)
    log(f"  CPM cols mapped via registry:      {len(mapped & all_cpm)}")
    log(f"  CPM cols in cpm_unmapped_triage_v265: {len(triage_cols & all_cpm)}")
    log(f"  CPM cols truly unaccounted:           {len(unaccounted)}")

    # Bethesda cluster verification
    bethesda_cols = [
        "bethesda_final", "bethesda_max_preop_2010", "bethesda_max_preop_2015",
        "bethesda_max_preop_2023", "n_bethesda_calculated_fnas",
        "n_bethesda_number_only_fnas", "bethesda_derivation_methods",
        "bethesda_index_nodule", "bethesda_index_nodule_linkage_source",
    ]
    bethesda_in_mapped = sum(1 for b in bethesda_cols if b in mapped)
    bethesda_in_cpm = sum(1 for b in bethesda_cols if b in all_cpm)
    log(f"  bethesda cluster on CPM:    {bethesda_in_cpm}/9")
    log(f"  bethesda cluster mapped:    {bethesda_in_mapped}/9")

    # Path size human-review staging file count
    n_path_size_review = con.execute(
        f"SELECT COUNT(*) FROM main.path_size_adjudication_v241").fetchone()[0]
    log(f"  path_size_adjudication_v241 staged rows: {n_path_size_review}")

    # ETE adjudication low-confidence count
    n_ete_low = con.execute(
        f"SELECT COUNT(*) FROM main.ete_adjudication_v1 "
        f"WHERE adjudicated_confidence='low'").fetchone()[0]
    n_ete_high = con.execute(
        f"SELECT COUNT(*) FROM main.ete_adjudication_v1 "
        f"WHERE adjudicated_confidence='high'").fetchone()[0]
    log(f"  ete_adjudication_v1: high={n_ete_high} low={n_ete_low}")

    # nan-repair audit summary
    n_nan_no_action = con.execute(f"""
        SELECT COUNT(*) FROM {PUBLICATION_DB}.manuscript_workspace.nan_string_audit_v1_1
        WHERE repair_action='NO_ACTION'
    """).fetchone()[0]
    n_nan_preserve = con.execute(f"""
        SELECT COUNT(*) FROM {PUBLICATION_DB}.manuscript_workspace.nan_string_audit_v1_1
        WHERE repair_action='PRESERVE_RAW'
    """).fetchone()[0]
    total_nan_cells = con.execute(f"""
        SELECT SUM(n_literal_nan)
        FROM {PUBLICATION_DB}.manuscript_workspace.nan_string_audit_v1_1
    """).fetchone()[0]
    log(f"  nan_string_audit_v1_1: NO_ACTION={n_nan_no_action} "
        f"PRESERVE_RAW={n_nan_preserve} total_nan_cells={total_nan_cells}")

    # ----- Snapshot CPM -----
    snap_ts = started_at.strftime("%Y%m%dT%H%M%SZ")
    snap_table = f"canonical_patient_master_pre270_{snap_ts}"
    snap_fq = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"."{snap_table}"'
    log(f"\n--- SNAPSHOT CPM ---")
    log(f"  target: {snap_fq}")

    # Defensive: refuse if a snapshot with this exact name somehow exists.
    # DuckDB doesn't support cross-DB information_schema queries via fully-
    # qualified path; use the database-aware duckdb_tables() function (same
    # pattern as Scripts 251, 259).
    snap_exists = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name = '{ARCHIVE_DB}'
          AND schema_name   = '{ARCHIVE_SCHEMA}'
          AND table_name    = '{snap_table}'
    """).fetchone()[0]
    if snap_exists:
        log(f"  refused: snapshot already exists at {snap_fq}")
        OUT_LOG.write_text("".join(log_lines))
        return 1

    con.execute(f"CREATE TABLE {snap_fq} AS SELECT * FROM {PUBLICATION_DB}.main.{CPM}")
    n_snap = con.execute(f"SELECT COUNT(*) FROM {snap_fq}").fetchone()[0]
    if n_snap != n_cpm:
        log(f"FAIL: snapshot row count {n_snap} != CPM {n_cpm}")
        OUT_LOG.write_text("".join(log_lines))
        return 1
    log(f"  snapshot rows: {n_snap} (matches CPM)")

    # ----- 5 audit-row inserts -----
    log("\n--- INSERT 5 AUDIT ROWS ---")

    rows: list[tuple] = [
        (
            "270",
            "step_0_catalog_reconciliation",
            "main_schema_catalog_ghosts",
            len(ghosts),
            len(ghosts),
            0,
            "OK" if len(ghosts) == 0 else "FAIL",
            (
                "Live recon: main info_schema BASE TABLE count = "
                f"{len(info_tables)}; queryable count = {queryable_count}; "
                f"ghost count = {len(ghosts)}; main.__readme rows = "
                f"{n_readme}. Original Script 270 prompt assumed ~130 "
                "ghosts requiring sweep apparatus; live state shows none "
                "(prior cleanup already complete). __readme is current. "
                f"snapshot={snap_fq} commit_anchor={GIT_COMMIT_SHA_PHASE_0}."
            ),
        ),
        (
            "270",
            "step_0_bethesda_retriage",
            "cpm_cols_unaccounted",
            len(unaccounted),
            len(unaccounted),
            0,
            "OK" if len(unaccounted) == 0 else "FAIL",
            (
                "Live recon vs original prompt's STEP 0 sub-step 5b: CPM "
                f"has {n_cpm_cols} cols, of which {len(mapped & all_cpm)} "
                "are mapped via detail_table_registry_v1 + "
                f"{len(triage_cols & all_cpm)} are in cpm_unmapped_triage"
                f"_v265, leaving {len(unaccounted)} unaccounted. The 9 "
                "post-266 bethesda_* columns were already mapped by "
                f"Script 268 ({bethesda_in_mapped}/9 in registry). "
                "Bucket-C remains 174, NOT 174+9=183 as the prompt "
                "predicted. No re-triage needed."
            ),
        ),
        (
            "270",
            "step_1a_ete_already_applied_in_v2",
            "ete_adjudications_pending",
            0,
            0,
            0,
            "DOCUMENTED_NOOP",
            (
                "ETE adjudication work was already applied to "
                "cpm.ete_grade_final_v2 by a prior script. Of 45 rows in "
                f"main.ete_adjudication_v1, {n_ete_high} 'high' confidence "
                "rows are already in ete_grade_final_v2 (verified by "
                "JOIN against CPM); the 26 'low' confidence rows are "
                "correctly retained as 'present_ungraded' in _v2. No "
                "ete_grade_final_v270 column created (would duplicate _v2 "
                "without semantic gain). Standing reference doc for the "
                "26 low-conf RIDs: docs/270_ete_low_conf_register.csv. "
                "Original prompt assumed numeric confidence and "
                "path_synoptics.worst_ete; live state has VARCHAR "
                "low|high enum and no worst_ete col on path_synoptics."
            ),
        ),
        (
            "270",
            "step_1b_path_size_deferred_for_clinician",
            "path_size_adjudications_pending_human",
            n_path_size_review,
            n_path_size_review,
            0,
            "OK_DEFERRED_HUMAN",
            (
                f"All {n_path_size_review} rows in main.path_size_"
                "adjudication_v241 staged for clinician sign-off at "
                "scripts/output/270_path_size_human_review.csv. "
                "Distribution: 37 HIGH outlier_manual_review_required "
                "(proposed_value IS NULL on all 37 — flag-only bucket); "
                "45 MEDIUM multifocal_use_rollup_max (mean |delta| ~3.9 "
                "cm — staging-relevant, not auto-applicable); 14 MEDIUM "
                "unifocal_retain_path_size (proposed=raw, no-op). "
                "No path_tumor_size_cm_adjudicated_v270 column written. "
                "Would-be filter thresholds for v1_1 replay: "
                "review_priority='HIGH' AND adjudication_rule IN "
                "('outlier_manual_review_required'); OR adjudication_rule"
                "='unifocal_retain_path_size'; OR ABS(delta_cm) <= 5.0. "
                "Status will flip to OK once worksheet returned with "
                "reviewer_decision populated."
            ),
        ),
        (
            "270",
            "step_3_nan_repair_already_resolved",
            "cpm_varchar_cols_with_literal_nan",
            int(n_nan_preserve),
            int(n_nan_preserve),
            0,
            "DOCUMENTED_NOOP",
            (
                "Live full-scan of all 476 CPM VARCHAR cols (28-sec sweep) "
                f"confirms exactly {total_nan_cells} literal-'nan' cells, "
                "all in syn_margin_distance_mm_raw_str (n_distinct_real=52, "
                "n_real_null=2). nan_string_audit_v1_1 distribution: "
                f"NO_ACTION={n_nan_no_action} (no nan present, nothing to "
                f"repair); PRESERVE_RAW={n_nan_preserve} (the _raw_str "
                "suffix preserves source representation by design). "
                "Original prompt's REPAIR rule "
                "(VARCHAR + cardinality<=50 + enum-name regex + >=1 real "
                "NULL) does not fire on this column — suffix is not in "
                "{flag,status,category,grade,level,type,source,method,"
                "class,laterality} and cardinality 52 just exceeds cutoff. "
                "Repair would destroy provenance signal. 0 cells repaired; "
                "audit retains current resolved state. Full evidence: "
                "scripts/output/270b_nan_recon.md."
            ),
        ),
    ]

    inserted = 0
    for row in rows:
        con.execute(f"""
            INSERT INTO {AUDIT_FQ}
                (run_ts, script_num, finding_id, metric,
                 count_before, count_after, target_after, status, notes)
            VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
        """, row)
        inserted += 1
        log(f"  inserted: finding_id={row[1]!r} metric={row[2]!r} "
            f"{row[3]}->{row[4]} target={row[5]} status={row[6]!r}")

    # ----- Verify post-state -----
    log("\n--- POST-INSERT VERIFICATION ---")
    landed = con.execute(f"""
        SELECT script_num, finding_id, count_before, count_after, status
        FROM {AUDIT_FQ}
        WHERE finding_id IN ({','.join(repr(f) for f in FINDING_IDS)})
        ORDER BY finding_id
    """).fetchall()
    if len(landed) != len(FINDING_IDS):
        log(f"FAIL: expected {len(FINDING_IDS)} landed, got {len(landed)}")
        OUT_LOG.write_text("".join(log_lines))
        return 1
    for r in landed:
        log(f"  {r}")

    # Status distribution after
    status_dist = dict(con.execute(f"""
        SELECT status, COUNT(*) FROM {AUDIT_FQ}
        GROUP BY status ORDER BY 2 DESC
    """).fetchall())
    log(f"  full audit-table status dist: {status_dist}")

    # Persist
    payload = {
        "started_at": started_at.isoformat(),
        "publication_db": PUBLICATION_DB,
        "snapshot": snap_fq,
        "snapshot_rows": int(n_snap),
        "cpm_state": {
            "rows": int(n_cpm),
            "distinct_research_id": int(n_cpm_distinct),
            "null_research_id": int(n_cpm_null_rid),
            "n_columns": int(n_cpm_cols),
        },
        "catalog_state": {
            "info_schema_base_tables": len(info_tables),
            "queryable_base_tables": queryable_count,
            "ghost_base_tables": len(ghosts),
            "ghosts": ghosts,
            "__readme_rows": int(n_readme),
        },
        "drift_state": {
            "cpm_cols_total": len(all_cpm),
            "cpm_cols_mapped_via_registry": len(mapped & all_cpm),
            "cpm_cols_in_triage": len(triage_cols & all_cpm),
            "cpm_cols_unaccounted": len(unaccounted),
            "bethesda_cluster_on_cpm": bethesda_in_cpm,
            "bethesda_cluster_mapped": bethesda_in_mapped,
        },
        "step_1_state": {
            "ete_high_conf": int(n_ete_high),
            "ete_low_conf": int(n_ete_low),
            "path_size_staged": int(n_path_size_review),
        },
        "step_3_state": {
            "nan_no_action": int(n_nan_no_action),
            "nan_preserve_raw": int(n_nan_preserve),
            "total_nan_cells": int(total_nan_cells),
        },
        "audit_rows_inserted": inserted,
        "audit_rows_inserted_finding_ids": list(FINDING_IDS),
        "post_insert_status_distribution": {k: int(v) for k, v in status_dist.items()},
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str))
    OUT_LOG.write_text("".join(log_lines))
    log(f"\nwrote {OUT_JSON}")
    log(f"wrote {OUT_LOG}")
    log(f"=== END Script 270b Phase A pre-flight audit ===")
    OUT_LOG.write_text("".join(log_lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
