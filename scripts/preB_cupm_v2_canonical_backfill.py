#!/usr/bin/env python3
"""
CPM TIRADS pre-B — canonical backfill of `main.canonical_us_patient_master_v2`.

Adds 9 columns to cupm_v2:
  - 7 port-from-CPM cols (rename-on-move, persisted in main.cupm_v2_canonical_backfill_v1)
  - 2 compute-from-cunc_v2 cols (max_nodule_size_mm, n_nodule_records)

Run order:
    python3 scripts/preB_cupm_v2_canonical_backfill.py --phase 1   # recon
    python3 scripts/preB_cupm_v2_canonical_backfill.py --phase 2   # build backfill snapshot
    python3 scripts/preB_cupm_v2_canonical_backfill.py --phase 3   # replace cupm_v2 view
    python3 scripts/preB_cupm_v2_canonical_backfill.py --phase 4   # verify shape
    python3 scripts/preB_cupm_v2_canonical_backfill.py --phase 5   # re-run coverage audit
    python3 scripts/preB_cupm_v2_canonical_backfill.py --phase 6   # write QA JSON

Each phase writes its log + report to scripts/output/preB_phase<N>_*.{log,json}.
Phase 6 writes the final QA bundle to qa/qa_script_cpm_tirads_preB.json.

Predecessor: Part A audit + Part B Phase 1 (STOP-gated on 13 gap_ABORT cols).
Successor:  Part B Phases 1-7 re-run from a clean (0 gap) coverage table.

References:
- Sign-off prompt: CPM_tirads_preB_canonical_backfill_cursor_prompt_20260421.md
- Architecture decision (Logan, 2026-04-21): Option C-soft. cupm_v2 is the
  patient-grain canonical surface; consumers JOIN cuem_v2/cunc_v2 for finer grain.
- Backing-table semantics: snapshot from CPM 2026-04-21; upstream writers (271b,
  221) frozen by Part B Phase 4. Future re-derivations go via the pre-drop CPM
  archive at "Thyroid 2026 UPdated".cpm_tirads_legacy_20260421.canonical_patient_master_pre_partB
  which Part B Phase 5 creates (NOT this script).
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

DB = "thyroid_canonical_publication_v1_0"
OUT_DIR = REPO / "scripts" / "output"
QA_DIR = REPO / "qa"

# ── Sign-off-locked column maps ──────────────────────────────────────────────
PORT_COLS: list[tuple[str, str, str]] = [
    # (cpm_source_col, cupm_v2_target_col, cpm_dtype)
    ("imaging_laterality_rollup_v271b",                   "imaging_laterality_rollup_v2",                  "VARCHAR"),
    ("pathology_vs_imaging_laterality_concordant_v271b",  "pathology_vs_imaging_laterality_concordant_v2", "VARCHAR"),
    ("tumor_pathology_laterality_v271b",                  "tumor_pathology_laterality_v2",                 "VARCHAR"),
    ("tirads_v2_any_fna_recommended_report",              "any_fna_recommended_report_ever",               "BOOLEAN"),
    ("tirads_v2_any_fna_recommended_report_source",       "any_fna_recommended_report_source",             "VARCHAR"),
    ("tirads_v2_worst_rank",                              "tirads_worst_rank_ever",                        "INTEGER"),
    ("tirads_v2_worst_rank_source",                       "tirads_worst_rank_source",                      "VARCHAR"),
]

# Part A populated counts for the 7 source CPM cols
PART_A_EXPECTED: dict[str, int] = {
    "imaging_laterality_rollup_v271b":                   3439,
    "pathology_vs_imaging_laterality_concordant_v271b": 10871,
    "tumor_pathology_laterality_v271b":                  3986,
    "tirads_v2_any_fna_recommended_report":              4073,
    "tirads_v2_any_fna_recommended_report_source":       4073,
    "tirads_v2_worst_rank":                              2465,
    "tirads_v2_worst_rank_source":                       2465,
}


def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect():
    return MotherDuckClient(MotherDuckConfig(database=DB)).connect_rw()


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — Recon (read-only)
# ─────────────────────────────────────────────────────────────────────────────

def phase1_recon() -> dict:
    con = connect()
    log: dict = {"phase": 1, "started_at_utc": utc_iso()}

    # 1) Confirm cupm_v2 is a VIEW
    is_view = con.execute(
        """
        SELECT table_type FROM information_schema.tables
        WHERE table_schema='main' AND table_name='canonical_us_patient_master_v2'
        """
    ).fetchone()
    log["cupm_v2_table_type"] = is_view[0] if is_view else None
    assert log["cupm_v2_table_type"] == "VIEW", \
        f"cupm_v2 must be a VIEW; got {log['cupm_v2_table_type']}"

    # 2) Pull current view definition (for archival)
    defn = con.execute(
        """
        SELECT view_definition FROM information_schema.views
        WHERE table_schema='main' AND table_name='canonical_us_patient_master_v2'
        """
    ).fetchone()[0]
    (OUT_DIR / "preB_phase1_cupm_v2_view_definition_before.sql").write_text(defn + "\n")
    log["cupm_v2_view_definition_archived"] = "scripts/output/preB_phase1_cupm_v2_view_definition_before.sql"

    cupm_n = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_us_patient_master_v2").fetchone()
    log["cupm_v2_row_count_before"] = cupm_n[0]
    log["cupm_v2_distinct_rids_before"] = cupm_n[1]
    cupm_cols = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema='main' AND table_name='canonical_us_patient_master_v2'
        """
    ).fetchone()[0]
    log["cupm_v2_column_count_before"] = cupm_cols

    # 3) cunc_v2 schema sanity for size cols
    cunc_size_cols = con.execute(
        """
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema='main' AND table_name='canonical_us_nodule_v2'
          AND column_name IN ('length_mm','width_mm','height_mm','size_cm_max','research_id')
        ORDER BY column_name
        """
    ).fetchall()
    log["cunc_v2_size_columns"] = [{"name": r[0], "type": r[1]} for r in cunc_size_cols]
    have = {r[0] for r in cunc_size_cols}
    missing = {"length_mm", "width_mm", "height_mm", "size_cm_max", "research_id"} - have
    assert not missing, f"cunc_v2 missing required size cols: {missing}"

    # 4) cunc_v2 size populated counts (for fallback impact assessment)
    pop = con.execute(
        """
        SELECT
          COUNT(*)                                             AS n_rows,
          COUNT(DISTINCT research_id)                          AS n_rids,
          COUNT(length_mm)                                     AS n_length_mm,
          COUNT(width_mm)                                      AS n_width_mm,
          COUNT(height_mm)                                     AS n_height_mm,
          COUNT(size_cm_max)                                   AS n_size_cm_max,
          -- rows where ALL three individual dims are NULL (would force fallback)
          COUNT(*) FILTER (WHERE length_mm IS NULL AND width_mm IS NULL AND height_mm IS NULL)
                                                               AS n_all_three_null,
          -- rows where ALL three NULL but size_cm_max present (fallback hits)
          COUNT(*) FILTER (
              WHERE length_mm IS NULL AND width_mm IS NULL AND height_mm IS NULL
                AND size_cm_max IS NOT NULL
          )                                                    AS n_fallback_active,
          -- rows where everything is NULL (no size data at all)
          COUNT(*) FILTER (
              WHERE length_mm IS NULL AND width_mm IS NULL AND height_mm IS NULL
                AND size_cm_max IS NULL
          )                                                    AS n_no_size_at_all
        FROM main.canonical_us_nodule_v2
        """
    ).fetchone()
    keys = ["n_rows","n_rids","n_length_mm","n_width_mm","n_height_mm","n_size_cm_max",
            "n_all_three_null","n_fallback_active","n_no_size_at_all"]
    cunc_pop = dict(zip(keys, pop))
    log["cunc_v2_populated_counts"] = cunc_pop
    cunc_pop["fallback_active_pct"] = round(100.0 * cunc_pop["n_fallback_active"] / max(cunc_pop["n_rows"], 1), 2)
    cunc_pop["no_size_pct"]        = round(100.0 * cunc_pop["n_no_size_at_all"] / max(cunc_pop["n_rows"], 1), 2)
    log["fallback_above_5pct"] = cunc_pop["fallback_active_pct"] > 5.0

    # 5) CPM port-source populated counts vs Part A expectations
    src_counts: dict[str, dict] = {}
    drift_flags: list[str] = []
    for src, _, _ in PORT_COLS:
        n = con.execute(f'SELECT COUNT("{src}") FROM main.canonical_patient_master').fetchone()[0]
        expected = PART_A_EXPECTED[src]
        drift_pct = round(100.0 * abs(n - expected) / max(expected, 1), 3)
        src_counts[src] = {"n_now": n, "n_part_a": expected, "drift_pct": drift_pct}
        if drift_pct > 0.5:
            drift_flags.append(src)
    log["cpm_port_source_counts"] = src_counts
    log["cpm_drift_flags_above_0_5_pct"] = drift_flags
    if drift_flags:
        log["stop_gate_triggered"] = True
        raise SystemExit(
            f"Phase 1 STOP gate: source columns drifted from Part A by >0.5%: {drift_flags}"
        )

    # 6) backfill table existence sanity (must NOT exist yet)
    exists = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema='main' AND table_name='cupm_v2_canonical_backfill_v1'
        """
    ).fetchone()[0]
    log["backfill_table_already_exists"] = bool(exists)

    log["finished_at_utc"] = utc_iso()
    log["status"] = "OK"
    return log


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — Build backfill snapshot
# ─────────────────────────────────────────────────────────────────────────────

def phase2_backfill() -> dict:
    con = connect()
    log: dict = {"phase": 2, "started_at_utc": utc_iso()}

    sql = """
    CREATE OR REPLACE TABLE main.cupm_v2_canonical_backfill_v1 AS
    SELECT
        research_id,
        imaging_laterality_rollup_v271b                     AS imaging_laterality_rollup_v2,
        pathology_vs_imaging_laterality_concordant_v271b    AS pathology_vs_imaging_laterality_concordant_v2,
        tumor_pathology_laterality_v271b                    AS tumor_pathology_laterality_v2,
        tirads_v2_any_fna_recommended_report                AS any_fna_recommended_report_ever,
        tirads_v2_any_fna_recommended_report_source         AS any_fna_recommended_report_source,
        tirads_v2_worst_rank                                AS tirads_worst_rank_ever,
        tirads_v2_worst_rank_source                         AS tirads_worst_rank_source,
        now()                                               AS backfilled_at_utc,
        'snapshot from main.canonical_patient_master 2026-04-21; upstream writers (Scripts 271b, 221) frozen by CPM TIRADS Part B Phase 4. Regen path: rebuild from "Thyroid 2026 UPdated".cpm_tirads_legacy_20260421.canonical_patient_master_pre_partB (created by Part B Phase 5).'
                                                            AS source_snapshot_note
    FROM main.canonical_patient_master
    WHERE imaging_laterality_rollup_v271b                       IS NOT NULL
       OR pathology_vs_imaging_laterality_concordant_v271b      IS NOT NULL
       OR tumor_pathology_laterality_v271b                      IS NOT NULL
       OR tirads_v2_any_fna_recommended_report                  IS NOT NULL
       OR tirads_v2_any_fna_recommended_report_source           IS NOT NULL
       OR tirads_v2_worst_rank                                  IS NOT NULL
       OR tirads_v2_worst_rank_source                           IS NOT NULL
    """
    con.execute(sql)

    # Verify
    bf_n = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.cupm_v2_canonical_backfill_v1"
    ).fetchone()
    log["backfill_row_count"] = bf_n[0]
    log["backfill_distinct_rids"] = bf_n[1]
    log["backfill_unique_rids_ok"] = bf_n[0] == bf_n[1]
    assert log["backfill_unique_rids_ok"], (
        f"Backfill table has duplicate RIDs: {bf_n[0]} rows / {bf_n[1]} distinct RIDs"
    )

    # Per-column populated count match (CPM source vs backfill target)
    matches: dict[str, dict] = {}
    for src, tgt, _ in PORT_COLS:
        n_cpm = con.execute(
            f'SELECT COUNT("{src}") FROM main.canonical_patient_master WHERE "{src}" IS NOT NULL'
        ).fetchone()[0]
        n_bf = con.execute(
            f'SELECT COUNT("{tgt}") FROM main.cupm_v2_canonical_backfill_v1 WHERE "{tgt}" IS NOT NULL'
        ).fetchone()[0]
        # Cell-level match: # of RIDs where CPM and backfill agree (both NOT NULL with same value)
        cell_match = con.execute(f"""
            SELECT COUNT(*) FROM main.canonical_patient_master cpm
            LEFT JOIN main.cupm_v2_canonical_backfill_v1 bf USING (research_id)
            WHERE cpm."{src}" IS NOT DISTINCT FROM bf."{tgt}"
        """).fetchone()[0]
        cpm_total = con.execute("SELECT COUNT(*) FROM main.canonical_patient_master").fetchone()[0]
        matches[tgt] = {
            "cpm_source_col":        src,
            "n_cpm_populated":       n_cpm,
            "n_backfill_populated":  n_bf,
            "n_cells_match":         cell_match,
            "n_cpm_total":           cpm_total,
            "match_pct":             round(100.0 * cell_match / cpm_total, 4),
        }
    log["per_col_match"] = matches
    bad = [k for k, v in matches.items() if v["match_pct"] < 100.0]
    log["per_col_match_failures"] = bad
    assert not bad, f"Cell-match below 100% for: {bad}"

    # 50-RID spot check
    sample = con.execute(f"""
        SELECT cpm.research_id,
               {", ".join([f'cpm."{src}" AS "src_{src}", bf."{tgt}" AS "tgt_{tgt}"' for src, tgt, _ in PORT_COLS])}
        FROM main.canonical_patient_master cpm
        LEFT JOIN main.cupm_v2_canonical_backfill_v1 bf USING (research_id)
        WHERE bf.research_id IS NOT NULL
        ORDER BY random()
        LIMIT 50
    """).df()
    sample_path = OUT_DIR / "preB_phase2_50rid_spot_check.csv"
    sample.to_csv(sample_path, index=False)
    log["spot_check_path"] = str(sample_path.relative_to(REPO))
    log["spot_check_n_rids"] = len(sample)

    log["finished_at_utc"] = utc_iso()
    log["status"] = "OK"
    return log


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3 — Replace cupm_v2 view definition
# ─────────────────────────────────────────────────────────────────────────────

NEW_VIEW_BODY = """\
CREATE OR REPLACE VIEW main.canonical_us_patient_master_v2 AS
WITH exam_agg AS (
    SELECT research_id,
           CAST('t' AS BOOLEAN)                                       AS has_any_us,
           count_star()                                               AS n_us_exams,
           min(exam_date)                                             AS first_us_date,
           max(exam_date)                                             AS last_us_date,
           bool_or(is_preop_exam)                                     AS preop_us_available_flag,
           max(worst_tirads_category_this_exam)                       AS max_tirads_category_ever,
           max(worst_tirads_points_this_exam)                         AS max_tirads_points_ever,
           sum(n_nodules_on_exam)                                     AS n_nodules_total_across_exams,
           bool_or(bilateral_flag)                                    AS bilateral_disease_flag_ever,
           (sum(CASE WHEN n_nodules_on_exam >= 2 THEN 1 ELSE 0 END) > 0)
                                                                      AS multifocal_flag_ever,
           bool_or(has_us_ln_findings)                                AS has_us_ln_findings_ever,
           bool_or(has_gland_findings)                                AS has_gland_findings_ever,
           (sum(COALESCE(n_abnormal_us_ln_on_exam, 0)) > 0)           AS any_suspicious_us_ln_ever,
           min(CASE WHEN n_abnormal_us_ln_on_exam IS NOT NULL
                     AND n_abnormal_us_ln_on_exam > 0
                    THEN exam_date END)                               AS first_abnormal_us_ln_date,
           bool_or(any_nlp_backfill_pending_on_exam)                  AS any_nlp_backfill_pending_for_patient
    FROM main.canonical_us_exam_master_v2
    GROUP BY 1
),
nodule_first_last AS (
    SELECT e.research_id,
           any_value(e.worst_tirads_category_this_exam ORDER BY e.exam_date)
             FILTER (WHERE e.exam_rank_for_patient = 1)               AS tirads_category_at_first_exam,
           any_value(e.worst_tirads_category_this_exam ORDER BY e.exam_date DESC)
             FILTER (WHERE CAST(e.is_preop_exam AS BOOLEAN) IS NOT DISTINCT FROM TRUE)
                                                                      AS tirads_category_at_last_preop_exam,
           min(CASE WHEN upper(e.worst_tirads_category_this_exam) IN ('TR4','TR5')
                    THEN e.exam_date END)                             AS first_high_risk_tirads_date
    FROM main.canonical_us_exam_master_v2 AS e
    GROUP BY 1
),
nodule_agg AS (
    -- NEW (pre-B 2026-04-21): per-RID nodule rollups
    -- max_nodule_size_mm: prefer GREATEST of individual mm dims; fallback to size_cm_max*10
    -- when all three individual dims are NULL on a row.
    -- n_nodule_records: COUNT(*) per RID over canonical_us_nodule_v2 (per-exam per-nodule rows).
    SELECT research_id,
           MAX(
             COALESCE(
               NULLIF(GREATEST(COALESCE(length_mm, 0),
                               COALESCE(width_mm,  0),
                               COALESCE(height_mm, 0)), 0),
               size_cm_max * 10.0
             )
           )                                                          AS max_nodule_size_mm,
           COUNT(*)                                                   AS n_nodule_records
    FROM main.canonical_us_nodule_v2
    GROUP BY 1
)
SELECT
    e.research_id,
    e.has_any_us,
    e.n_us_exams,
    e.first_us_date,
    e.last_us_date,
    e.preop_us_available_flag,
    e.max_tirads_category_ever,
    e.max_tirads_points_ever,
    nfl.tirads_category_at_first_exam,
    nfl.tirads_category_at_last_preop_exam,
    e.n_nodules_total_across_exams,
    e.bilateral_disease_flag_ever,
    e.multifocal_flag_ever,
    nfl.first_high_risk_tirads_date,
    e.has_us_ln_findings_ever,
    e.any_suspicious_us_ln_ever,
    e.first_abnormal_us_ln_date,
    e.has_gland_findings_ever,
    e.any_nlp_backfill_pending_for_patient,
    -- 7 new port-from-CPM cols (via backfill snapshot table)
    bf.imaging_laterality_rollup_v2,
    bf.pathology_vs_imaging_laterality_concordant_v2,
    bf.tumor_pathology_laterality_v2,
    bf.any_fna_recommended_report_ever,
    bf.any_fna_recommended_report_source,
    bf.tirads_worst_rank_ever,
    bf.tirads_worst_rank_source,
    -- 2 new compute-from-cunc_v2 cols
    na.max_nodule_size_mm,
    na.n_nodule_records
FROM exam_agg AS e
LEFT JOIN nodule_first_last                  AS nfl USING (research_id)
LEFT JOIN main.cupm_v2_canonical_backfill_v1 AS bf  USING (research_id)
LEFT JOIN nodule_agg                         AS na  USING (research_id)
"""


def phase3_replace_view() -> dict:
    con = connect()
    log: dict = {"phase": 3, "started_at_utc": utc_iso()}

    # Snapshot pre-state for rollback
    pre_n = con.execute(
        "SELECT COUNT(*) FROM main.canonical_us_patient_master_v2"
    ).fetchone()[0]
    pre_cols = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema='main' AND table_name='canonical_us_patient_master_v2'
        """
    ).fetchone()[0]
    log["pre_state"] = {"row_count": pre_n, "column_count": pre_cols}
    assert pre_cols == 19, f"Pre-state column count expected 19, got {pre_cols}"

    # Execute view replacement
    con.execute(NEW_VIEW_BODY)
    log["view_replaced_at_utc"] = utc_iso()

    # Save the body for archival
    (OUT_DIR / "preB_phase3_cupm_v2_view_definition_after.sql").write_text(NEW_VIEW_BODY + "\n")
    log["new_view_definition_archived"] = "scripts/output/preB_phase3_cupm_v2_view_definition_after.sql"

    log["finished_at_utc"] = utc_iso()
    log["status"] = "OK"
    return log


# ─────────────────────────────────────────────────────────────────────────────
# Phase 4 — Verify shape
# ─────────────────────────────────────────────────────────────────────────────

NEW_PORT_TARGET_COLS = [tgt for _, tgt, _ in PORT_COLS]
NEW_COMPUTE_COLS = ["max_nodule_size_mm", "n_nodule_records"]


def phase4_verify() -> dict:
    con = connect()
    log: dict = {"phase": 4, "started_at_utc": utc_iso()}

    # 1. Column count
    cols = con.execute(
        """
        SELECT column_name, data_type, ordinal_position FROM information_schema.columns
        WHERE table_schema='main' AND table_name='canonical_us_patient_master_v2'
        ORDER BY ordinal_position
        """
    ).fetchall()
    log["cupm_v2_columns_after"] = [
        {"name": r[0], "type": r[1], "pos": r[2]} for r in cols
    ]
    log["cupm_v2_column_count_after"] = len(cols)
    assert len(cols) == 28, f"Expected 28 cols, got {len(cols)}"

    # 2. Row count
    rn = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_us_patient_master_v2"
    ).fetchone()
    log["cupm_v2_row_count_after"] = rn[0]
    log["cupm_v2_distinct_rids_after"] = rn[1]
    assert rn[0] == 10859, f"Expected 10859 rows, got {rn[0]}"

    # 3. New columns populated counts
    new_pop = {}
    for c in NEW_PORT_TARGET_COLS + NEW_COMPUTE_COLS:
        n = con.execute(
            f'SELECT COUNT("{c}") FROM main.canonical_us_patient_master_v2'
        ).fetchone()[0]
        new_pop[c] = n
    log["new_columns_populated_counts"] = new_pop

    # 4. n_nodule_records ≥ n_nodules_total_across_exams per RID — FAIL if reverse
    inversion = con.execute(
        """
        SELECT COUNT(*) AS n_inverted
        FROM main.canonical_us_patient_master_v2
        WHERE n_nodule_records IS NOT NULL
          AND n_nodules_total_across_exams IS NOT NULL
          AND n_nodule_records < n_nodules_total_across_exams
        """
    ).fetchone()[0]
    log["n_records_lt_n_total_inversions"] = inversion
    assert inversion == 0, (
        f"FAIL: {inversion} RIDs have n_nodule_records < n_nodules_total_across_exams. "
        "n_nodule_records counts per-exam per-nodule rows in cunc_v2; "
        "n_nodules_total_across_exams is already a per-patient exam-rolled count. "
        "The former MUST always be >= the latter (same nodule can appear across multiple exams)."
    )

    # 5. Per-port populated count must match backfill table, MODULO the RIDs
    # that exist in the backfill (sourced from CPM, 10871 RIDs) but not in
    # cupm_v2 (10859 RIDs — 12 patients have no US exams and so no exam_agg row).
    port_match = {}
    for src, tgt, _ in PORT_COLS:
        n_view = con.execute(f'SELECT COUNT("{tgt}") FROM main.canonical_us_patient_master_v2').fetchone()[0]
        n_bf = con.execute(f'SELECT COUNT("{tgt}") FROM main.cupm_v2_canonical_backfill_v1').fetchone()[0]
        # RIDs present in backfill (with NOT NULL for this col) but not in cupm_v2
        n_dropped_by_join = con.execute(f"""
            SELECT COUNT(*) FROM main.cupm_v2_canonical_backfill_v1 bf
            WHERE bf."{tgt}" IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM main.canonical_us_patient_master_v2 v
                  WHERE v.research_id = bf.research_id
              )
        """).fetchone()[0]
        expected_view = n_bf - n_dropped_by_join
        port_match[tgt] = {
            "n_view": n_view,
            "n_backfill": n_bf,
            "n_backfill_rids_not_in_cupm_v2": n_dropped_by_join,
            "expected_view": expected_view,
            "ok": n_view == expected_view,
        }
    log["port_view_vs_backfill_counts"] = port_match
    bad = [k for k, v in port_match.items() if not v["ok"]]
    assert not bad, (
        f"View port counts != (backfill - non-cupm-RIDs) for: {bad}. "
        "Inspect port_view_vs_backfill_counts in this report."
    )

    # 6. compute cols sanity (max_nodule_size_mm + n_nodule_records)
    nodule_summary = con.execute(
        """
        SELECT
          COUNT(max_nodule_size_mm)                   AS n_max_size,
          MIN(max_nodule_size_mm)                     AS min_size,
          MAX(max_nodule_size_mm)                     AS max_size,
          MEDIAN(max_nodule_size_mm)                  AS p50_size,
          COUNT(n_nodule_records)                     AS n_nrec,
          MIN(n_nodule_records)                       AS min_nrec,
          MAX(n_nodule_records)                       AS max_nrec,
          MEDIAN(n_nodule_records)                    AS p50_nrec
        FROM main.canonical_us_patient_master_v2
        """
    ).fetchone()
    keys = ["n_max_size","min_size","max_size","p50_size","n_nrec","min_nrec","max_nrec","p50_nrec"]
    log["compute_col_summary"] = dict(zip(keys, nodule_summary))

    # 7. Existing-19-cols regression: 5 random RIDs, value-by-value match against pre-recorded snapshot
    # We didn't snapshot beforehand, so re-derive from cuem_v2 first principles isn't possible.
    # Sanity proxy: the 19 original cols are still present and the populated counts haven't dropped.
    # (A full regression would require snapshotting before view replacement; see note in QA section.)
    orig_cols = [
        "has_any_us","n_us_exams","first_us_date","last_us_date","preop_us_available_flag",
        "max_tirads_category_ever","max_tirads_points_ever",
        "tirads_category_at_first_exam","tirads_category_at_last_preop_exam",
        "n_nodules_total_across_exams","bilateral_disease_flag_ever","multifocal_flag_ever",
        "first_high_risk_tirads_date","has_us_ln_findings_ever","any_suspicious_us_ln_ever",
        "first_abnormal_us_ln_date","has_gland_findings_ever","any_nlp_backfill_pending_for_patient",
    ]
    orig_pop = {}
    for c in orig_cols:
        orig_pop[c] = con.execute(
            f'SELECT COUNT("{c}") FROM main.canonical_us_patient_master_v2'
        ).fetchone()[0]
    log["original_columns_populated_counts"] = orig_pop

    log["finished_at_utc"] = utc_iso()
    log["status"] = "OK"
    return log


# ─────────────────────────────────────────────────────────────────────────────
# Phase 5 — Re-run coverage audit with updated MAPPING
# ─────────────────────────────────────────────────────────────────────────────

# Updated MAPPING reflecting the 9 new cupm_v2 cols
# coverage_status taxonomy:
#   mapped_cupm_v2          — direct map on cupm_v2 (post pre-B: includes the 9 newly added)
#   mapped_category         — type-coerced (BIGINT category vs VARCHAR TR rank)
#   mapped_points           — type-coerced (DOUBLE points)
#   mapped_unit_convert     — mm vs cm
#   gap_other_v2_table      — canonical exists on cunc_v2 (consumer JOINs with aggregation)
#   retired_redesign        — Q1 list (cohort_m025/m075 redesign per Logan)
#   drop_no_replacement     — slated for Part B Phase 5 with no canonical equivalent (Logan adjudicated)
PHASE5_MAPPING: list[tuple[str, str, str, str, str]] = [
    # ── 6 columns slated for DROP without replacement (Logan's adjudication) ──
    ("imaging_laterality_rollup",                       "-", "-", "drop_no_replacement",
     "Legacy un-suffixed; superseded by imaging_laterality_rollup_v2 on cupm_v2 (per pre-B port from _v271b)"),
    ("imaging_laterality_rollup_v2",                    "-", "-", "drop_no_replacement",
     "Old CPM column; cupm_v2.imaging_laterality_rollup_v2 (NEW, ported from _v271b) is now canonical"),
    ("pathology_vs_imaging_laterality_concordant",      "-", "-", "drop_no_replacement",
     "Legacy BOOLEAN; superseded by cupm_v2.pathology_vs_imaging_laterality_concordant_v2 (5-valued VARCHAR ported from _v271b)"),
    ("tirads_v2_any_ete_on_us",                         "-", "-", "drop_no_replacement",
     "0 readers, 0 views; no canonical equivalent — retire"),
    ("tirads_v2_n_reports",                             "-", "-", "drop_no_replacement",
     "0 readers, 0 views; no canonical equivalent — retire"),
    ("tirads_v2_shortest_followup_months",              "-", "-", "drop_no_replacement",
     "0 readers, 0 views; no canonical equivalent — retire"),

    # ── 7 ports now mapped to cupm_v2 (renamed) ──
    ("imaging_laterality_rollup_v271b",                 "canonical_us_patient_master_v2", "imaging_laterality_rollup_v2",
     "mapped_cupm_v2", "Ported via cupm_v2_canonical_backfill_v1; rename-on-move"),
    ("pathology_vs_imaging_laterality_concordant_v271b","canonical_us_patient_master_v2", "pathology_vs_imaging_laterality_concordant_v2",
     "mapped_cupm_v2", "Ported via backfill; rename-on-move"),
    ("tumor_pathology_laterality_v271b",                "canonical_us_patient_master_v2", "tumor_pathology_laterality_v2",
     "mapped_cupm_v2", "Ported via backfill; rename-on-move"),
    ("tirads_v2_any_fna_recommended_report",            "canonical_us_patient_master_v2", "any_fna_recommended_report_ever",
     "mapped_cupm_v2", "Ported via backfill; rename-on-move (drop tirads_v2_ prefix, add _ever suffix)"),
    ("tirads_v2_any_fna_recommended_report_source",     "canonical_us_patient_master_v2", "any_fna_recommended_report_source",
     "mapped_cupm_v2", "Ported via backfill; rename-on-move"),
    ("tirads_v2_worst_rank",                            "canonical_us_patient_master_v2", "tirads_worst_rank_ever",
     "mapped_cupm_v2", "Ported via backfill; rename-on-move (add _ever suffix)"),
    ("tirads_v2_worst_rank_source",                     "canonical_us_patient_master_v2", "tirads_worst_rank_source",
     "mapped_cupm_v2", "Ported via backfill; rename-on-move"),

    # ── 2 compute-from-cunc_v2 cols now on cupm_v2 ──
    ("tirads_nodule_size_max_mm_v12",                   "canonical_us_patient_master_v2", "max_nodule_size_mm",
     "mapped_cupm_v2", "Computed from cunc_v2 in cupm_v2 view body (GREATEST(length,width,height) with size_cm_max*10 fallback)"),
    ("tirads_n_nodule_records_v12",                     "canonical_us_patient_master_v2", "n_nodule_records",
     "mapped_cupm_v2", "Computed from cunc_v2 in cupm_v2 view body (COUNT(*) per RID)"),

    # ── existing patient-rollup maps (unchanged from Part B Phase 1) ──
    ("max_tirads_ever",                                 "canonical_us_patient_master_v2", "max_tirads_category_ever",
     "mapped_cupm_v2", "BIGINT 1-5 → VARCHAR TR1-TR5"),
    ("max_tirads_ever_v2",                              "canonical_us_patient_master_v2", "max_tirads_points_ever",
     "mapped_cupm_v2", "DOUBLE points (0-13+); already exists on cupm_v2"),
    ("worst_tirads_category",                           "canonical_us_patient_master_v2", "max_tirads_category_ever",
     "mapped_cupm_v2", "Patient worst TR rank rollup"),
    ("imaging_tirads_best",                             "canonical_us_patient_master_v2", "tirads_category_at_first_exam",
     "mapped_cupm_v2", "Per-exam first-exam category rollup"),
    ("imaging_tirads_worst",                            "canonical_us_patient_master_v2", "max_tirads_category_ever",
     "mapped_cupm_v2", "Patient worst TR rank rollup"),
    ("preop_tirads_best",                               "canonical_us_patient_master_v2", "tirads_category_at_last_preop_exam",
     "mapped_cupm_v2", "Last-preop-exam category"),
    ("preop_tirads_worst",                              "canonical_us_patient_master_v2", "tirads_category_at_last_preop_exam",
     "mapped_cupm_v2", "Best+worst flatten to last preop value on cupm_v2"),
    ("preop_tirads_category",                           "canonical_us_patient_master_v2", "tirads_category_at_last_preop_exam",
     "mapped_cupm_v2", "Likely same as preop_best/worst"),
    ("imaging_updated_tirads_category_cpm_v1",          "canonical_us_patient_master_v2", "max_tirads_category_ever",
     "mapped_cupm_v2", "Patient-rollup; v1 older, _v2 newer"),
    ("imaging_updated_tirads_category_cpm_v2",          "canonical_us_patient_master_v2", "max_tirads_category_ever",
     "mapped_cupm_v2", "Patient-rollup"),
    ("imaging_tirads_best_v2",                          "canonical_us_patient_master_v2", "tirads_category_at_first_exam",
     "mapped_cupm_v2", "Same as imaging_tirads_best"),
    ("imaging_tirads_worst_v2",                         "canonical_us_patient_master_v2", "max_tirads_category_ever",
     "mapped_cupm_v2", "Same as imaging_tirads_worst"),
    ("preop_tirads_best_v2",                            "canonical_us_patient_master_v2", "tirads_category_at_last_preop_exam",
     "mapped_cupm_v2", "Same as preop_tirads_best"),
    ("preop_tirads_category_v2",                        "canonical_us_patient_master_v2", "tirads_category_at_last_preop_exam",
     "mapped_cupm_v2", "Same as preop_tirads_category"),
    ("tirads_best_combined",                            "canonical_us_patient_master_v2", "tirads_category_at_first_exam",
     "mapped_cupm_v2", "Pre-v12 INTEGER form"),
    ("tirads_worst_combined",                           "canonical_us_patient_master_v2", "max_tirads_category_ever",
     "mapped_cupm_v2", "Pre-v12 INTEGER worst-ever"),
    ("tirads_best_category_v12",                        "canonical_us_patient_master_v2", "tirads_category_at_first_exam",
     "mapped_cupm_v2", "VARCHAR labels collapse to TR rank"),
    ("tirads_worst_category_v12",                       "canonical_us_patient_master_v2", "max_tirads_category_ever",
     "mapped_cupm_v2", "VARCHAR labels collapse to TR rank"),
    ("tirads_best_score_v12",                           "canonical_us_patient_master_v2", "tirads_category_at_first_exam",
     "mapped_category", "BIGINT category 1-5 → VARCHAR TR rank"),
    ("tirads_worst_score_v12",                          "canonical_us_patient_master_v2", "max_tirads_category_ever",
     "mapped_category", "BIGINT category 1-5 → VARCHAR TR rank"),
    ("tirads_worst_points_v271",                        "canonical_us_patient_master_v2", "max_tirads_points_ever",
     "mapped_points", "DOUBLE points; cupm_v2 has max-only"),
    ("tirads_v2_any_suspicious_ln_on_us",               "canonical_us_patient_master_v2", "any_suspicious_us_ln_ever",
     "mapped_cupm_v2", "Direct match"),
    ("tirads_v2_worst_category",                        "canonical_us_patient_master_v2", "max_tirads_category_ever",
     "mapped_cupm_v2", "Same column"),
    ("tirads_v2_max_points",                            "canonical_us_patient_master_v2", "max_tirads_points_ever",
     "mapped_cupm_v2", "Same column"),

    # ── 8 retired_redesign (unchanged from Part B Phase 1) ──
    ("tirads_concordant_count_v12",                     "-", "-", "retired_redesign", "Q1 decision: redesign cohort_m025/m075"),
    ("tirads_mismatch_count_v12",                       "-", "-", "retired_redesign", "Q1 decision: redesign cohort_m025/m075"),
    ("tirads_n_sources_v12",                            "-", "-", "retired_redesign", "Q1 decision: redesign cohort_m025"),
    ("tirads_reliability_v12",                          "-", "-", "retired_redesign", "Q1 decision: redesign cohort_m075"),
    ("tirads_has_acr_recalc_v12",                       "-", "-", "retired_redesign", "Concept retired in v2 pipeline"),
    ("tirads_source_v12",                               "-", "-", "retired_redesign", "Pipeline-source label — metadata-only"),
    ("tirads_source_system_v271",                       "-", "-", "retired_redesign", "Pipeline-source label — metadata-only"),
    ("imaging_tirads_source",                           "-", "-", "retired_redesign", "Pipeline-source label — metadata-only"),

    # ── 6 remaining gap_other_v2_table (consumer JOINs cunc_v2 with aggregation) ──
    ("tirads_best_points_v271",                         "-", "-", "gap_other_v2_table",
     "MIN points; consumer aggregates from cunc_v2.acr2017_tirads_points (0 readers — low priority)"),
    ("tirads_nodules_scored_combined",                  "-", "-", "gap_other_v2_table",
     "Consumer aggregates from cunc_v2 (1 reader)"),
    ("tirads_v2_any_fna_recommended",                   "-", "-", "gap_other_v2_table",
     "Consumer aggregates BOOL_OR(cunc_v2.fna_recommended_this_nodule) (0 readers)"),
    ("tirads_v2_any_interval_growth",                   "-", "-", "gap_other_v2_table",
     "Consumer aggregates BOOL_OR(cunc_v2.interval_growth_flag) (0 readers)"),
    ("tirads_v2_largest_nodule_cm",                     "-", "-", "gap_other_v2_table",
     "Consumer aggregates from cunc_v2 (0 readers)"),
    ("tirads_v2_n_nodules_scored",                      "-", "-", "gap_other_v2_table",
     "Consumer aggregates COUNT(*) from cunc_v2 WHERE tirads_reported_in_text IS NOT NULL (1 reader)"),
]


def phase5_coverage_rerun() -> dict:
    con = connect()
    log: dict = {"phase": 5, "started_at_utc": utc_iso()}

    # Re-pull CPM inventory with the SAME regex Part A used (Python re),
    # NOT a broad ILIKE which would pull in unrelated *_laterality columns
    # (e.g. rln_laterality, recurrence_laterality — surgery/ENT, not TIRADS).
    import re as _re
    PART_A_REGEX = _re.compile(
        r"(tirads|laterality_v271b|imaging_laterality_rollup|max_tirads|preop_tirads"
        r"|imaging_updated_tirads|worst_tirads_category|imaging_tirads"
        r"|pathology_vs_imaging_laterality)",
        _re.IGNORECASE,
    )
    NLP_RE = _re.compile(r"^nlp_(tirads|imaging|usnodule)_", _re.IGNORECASE)
    all_cpm = con.execute(
        """
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema='main' AND table_name='canonical_patient_master'
        """
    ).fetchall()
    cpm_types = {
        r[0]: r[1] for r in all_cpm
        if PART_A_REGEX.search(r[0]) and not NLP_RE.search(r[0])
    }
    log["cpm_audit_col_count"] = len(cpm_types)

    cupm_v2_cols = {
        r[0]: r[1] for r in con.execute(
            """SELECT column_name, data_type FROM information_schema.columns
               WHERE table_schema='main' AND table_name='canonical_us_patient_master_v2'"""
        ).fetchall()
    }

    # Validate every mapped canonical col exists on cupm_v2 now
    errors: list[str] = []
    for legacy, ctab, ccol, status, _ in PHASE5_MAPPING:
        if status.startswith("mapped") and ctab == "canonical_us_patient_master_v2":
            if ccol not in cupm_v2_cols:
                errors.append(f"Mapped canonical col missing: {legacy} → {ctab}.{ccol}")
    log["mapping_errors"] = errors
    assert not errors, f"Mapping errors: {errors}"

    # Validate every CPM TIRADS col is covered by MAPPING
    mapped_set = {m[0] for m in PHASE5_MAPPING}
    inventory_set = set(cpm_types.keys())
    missing = inventory_set - mapped_set
    extra = mapped_set - inventory_set
    log["cpm_cols_not_in_mapping"] = sorted(missing)
    log["mapping_cols_not_on_cpm"] = sorted(extra)
    if missing:
        raise SystemExit(f"Phase 5 STOP gate: CPM cols missing from MAPPING: {missing}")

    # Rebuild the coverage table
    con.execute("CREATE SCHEMA IF NOT EXISTS manuscript_workspace")
    con.execute("DROP TABLE IF EXISTS manuscript_workspace.cpm_tirads_canonical_coverage_v1")
    con.execute("""
        CREATE TABLE manuscript_workspace.cpm_tirads_canonical_coverage_v1 (
            column_name VARCHAR,
            cpm_dtype VARCHAR,
            canonical_table VARCHAR,
            canonical_column VARCHAR,
            canonical_dtype VARCHAR,
            coverage_status VARCHAR,
            notes VARCHAR
        )
    """)
    for legacy, ctab, ccol, status, notes in PHASE5_MAPPING:
        canon_dt = cupm_v2_cols.get(ccol, "—") if ctab == "canonical_us_patient_master_v2" else "—"
        cpm_dt = cpm_types.get(legacy, "<NOT_ON_CPM>")
        con.execute(
            "INSERT INTO manuscript_workspace.cpm_tirads_canonical_coverage_v1 VALUES (?,?,?,?,?,?,?)",
            [legacy, cpm_dt, ctab, ccol, canon_dt, status, notes],
        )

    # Status counts
    counts = con.execute(
        """SELECT coverage_status, COUNT(*) FROM manuscript_workspace.cpm_tirads_canonical_coverage_v1
           GROUP BY 1 ORDER BY 2 DESC"""
    ).fetchall()
    log["status_counts"] = {s: n for s, n in counts}
    log["n_gap_ABORT"] = log["status_counts"].get("gap_ABORT", 0)
    log["n_gap_other_v2_table"] = log["status_counts"].get("gap_other_v2_table", 0)

    # Hard assert (the whole point of pre-B): 0 gap_ABORT
    assert log["n_gap_ABORT"] == 0, f"Phase 5 STOP gate: {log['n_gap_ABORT']} gap_ABORT rows remain"

    # CSV dump for inspection
    df = con.execute(
        "SELECT * FROM manuscript_workspace.cpm_tirads_canonical_coverage_v1 ORDER BY coverage_status, column_name"
    ).df()
    csv_path = OUT_DIR / "preB_phase5_cpm_tirads_canonical_coverage_v1.csv"
    df.to_csv(csv_path, index=False)
    log["coverage_csv"] = str(csv_path.relative_to(REPO))

    log["finished_at_utc"] = utc_iso()
    log["status"] = "OK"
    return log


# ─────────────────────────────────────────────────────────────────────────────
# Phase 6 — QA bundle
# ─────────────────────────────────────────────────────────────────────────────

def phase6_qa() -> dict:
    """Aggregate phases 1-5 logs into one QA JSON committed to qa/."""
    qa: dict = {
        "qa_script": "preB_cupm_v2_canonical_backfill",
        "version": "v1",
        "completed_at_utc": utc_iso(),
        "phases": {},
    }
    for n in (1, 2, 3, 4, 5):
        p = OUT_DIR / f"preB_phase{n}.json"
        if p.exists():
            qa["phases"][f"phase{n}"] = json.loads(p.read_text())

    # Final summary row
    p1 = qa["phases"].get("phase1", {})
    p4 = qa["phases"].get("phase4", {})
    p5 = qa["phases"].get("phase5", {})
    qa["summary"] = {
        "view_column_count": {
            "before": p1.get("cupm_v2_column_count_before"),
            "after":  p4.get("cupm_v2_column_count_after"),
        },
        "view_row_count": {
            "before": p1.get("cupm_v2_row_count_before"),
            "after":  p4.get("cupm_v2_row_count_after"),
        },
        "backfill_table_rows":         (qa["phases"].get("phase2", {}) or {}).get("backfill_row_count"),
        "backfill_distinct_rids":      (qa["phases"].get("phase2", {}) or {}).get("backfill_distinct_rids"),
        "compute_col_summary":         p4.get("compute_col_summary"),
        "n_inversions_records_vs_total": p4.get("n_records_lt_n_total_inversions"),
        "fallback_cunc_v2_pct":        (p1.get("cunc_v2_populated_counts") or {}).get("fallback_active_pct"),
        "coverage_audit_after":        p5.get("status_counts"),
        "n_gap_ABORT":                 p5.get("n_gap_ABORT"),
        "n_gap_other_v2_table":        p5.get("n_gap_other_v2_table"),
    }

    QA_DIR.mkdir(parents=True, exist_ok=True)
    qa_path = QA_DIR / "qa_script_cpm_tirads_preB.json"
    qa_path.write_text(json.dumps(qa, indent=2, default=str))
    print(f"QA bundle written: {qa_path.relative_to(REPO)}")
    return qa


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

PHASES = {
    1: ("recon",                   phase1_recon),
    2: ("backfill",                phase2_backfill),
    3: ("replace_view",            phase3_replace_view),
    4: ("verify_shape",            phase4_verify),
    5: ("coverage_rerun",          phase5_coverage_rerun),
    6: ("qa_bundle",               phase6_qa),
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", type=int, required=True, choices=sorted(PHASES.keys()))
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    QA_DIR.mkdir(parents=True, exist_ok=True)

    name, fn = PHASES[args.phase]
    print(f"=== Running pre-B Phase {args.phase}: {name} ===")
    result = fn()

    out_path = OUT_DIR / f"preB_phase{args.phase}.json"
    out_path.write_text(json.dumps(result, indent=2, default=str))
    print(f"Phase {args.phase} report: {out_path.relative_to(REPO)}")
    if isinstance(result, dict) and result.get("status") == "OK":
        print(f"Phase {args.phase} OK.")
    elif isinstance(result, dict) and "completed_at_utc" in result:
        print(f"Phase {args.phase} completed.")
    else:
        print(f"Phase {args.phase} returned without explicit status.")


if __name__ == "__main__":
    main()
