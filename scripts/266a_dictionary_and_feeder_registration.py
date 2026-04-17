#!/usr/bin/env python3
"""
Script 266a — Dictionary governance + feeder registration + triage rebuild
=========================================================================

Scope (P1, P3, P4 of the 266 prompt; ZERO destructive ops):
  - Phase 0  : Connect, log, decision_log entries for preflight divergences.
  - Phase 1  : Snapshot data_dictionary_v240 to
               "Thyroid 2026 UPdated".archive_pub_v1_0.<t>_pre266a_<UTC>.
  - Phase 2  : Build data_dictionary_v266a as CTAS from v240 and apply
               status / replacement / description updates for:
                 * n_tumors            -> deprecated, replacement n_tumors_path
                 * n_tumors_v10        -> deprecated, replacement n_tumors_path
                 * gm_path_stage_raw   -> deprecated, no replacement
                 * gm_path_m_stage_raw -> deprecated, replacement path_m_stage_raw
                 * ajcc7_m_stage / ajcc8_m_stage -> annotate M0-default-fill
                 * has_left_tumor / has_right_tumor / has_isthmus_tumor
                       -> annotate feeder = tumor_pathology.tumor_laterality_overall
                 * n_tumors_ete_present, n_tumors_lvi_present,
                   n_tumors_margin_involved, n_tumors_margin_uninvolved,
                   n_tumors_with_size -> annotate feeder = patient_tumor_rollup_v1
  - Phase 3  : COMMENT ON COLUMN on the same CPM columns so the description
               travels with the table.
  - Phase 4  : UPDATE detail_table_registry_v1.tumor_pathology row to append
               has_left_tumor;has_right_tumor;has_isthmus_tumor to
               feeds_master_columns_normalized + feeds_master_columns
               (per the prompt's STEP 5 correction;
               the columns are also still listed under
               patient_tumor_rollup_v1 from script 245+, and that's fine —
               canonical_detail_pointer_v1 supports multi-feeder rows).
  - Phase 5  : Rebuild manuscript_workspace.cpm_unmapped_triage_v266a using
               canonical_detail_pointer_v1 (live VIEW) plus a
               dictionary-aware exclusion (status='deprecated' in v266a).
  - Phase 6  : Final acceptance gates.
                 * v266a row count == v240 row count
                 * deprecated entries in v266a >= 4 (n_tumors, n_tumors_v10,
                   gm_path_stage_raw, gm_path_m_stage_raw)
                 * cpm_unmapped_triage_v266a C bucket <= 165
                 * Staging-adjacent C entries gone:
                       has_*_tumor, n_tumors_*_present,
                       gm_path_stage_raw, gm_path_m_stage_raw
                 * All 65 manuscript_workspace VIEWs still SELECT 1 cleanly
                   (smoke check; we did not modify their referenced columns).

Flags:
  --dry-run     Default. Logs every planned DDL but executes none.
  --apply       Execute writes.
  --phase N     Run a single phase in isolation (0..6). Phase 0 always runs.

Outputs (scripts/output/):
  266a_run_log.md
  266a_decision_log.json
  266a_final_confirmation.json
  266a_view_smoke_check.csv
  266a_summary.md

Conventions honored (per __conventions live state, not stale prompt copy):
  - pre_flight_decision_log : every preflight discovery diverging from the
    prompt is logged in 266a_decision_log.json.
  - rid_type_consistency    : not applicable here (no CPM-side joins; all
    UPDATEs are within data_dictionary_v240 / detail_table_registry_v1
    on column_name keys).
  - main_schema_keep_list   : v266a is a NEW table in main; v240 is left
    in place (266c will retire v240 if/when v266 supersedes it).

Notes:
  * NO destructive ops. NO RENAME. NO DROP TABLE on production tables.
    NO ALTER TABLE on raw_source-tagged tables. NO ADD COLUMN to CPM.
    Only writes are: CTAS to archive (snapshot), CTAS to main (v266a),
    UPDATE on v266a + detail_table_registry_v1, COMMENT ON COLUMN on CPM,
    CREATE OR REPLACE TABLE on cpm_unmapped_triage_v266a.
  * data_dictionary_v240 is NOT modified in this pass. The deprecation
    edits live in v266a only. v240 stays authoritative for any consumer
    that hasn't been switched.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import (  # noqa: E402
    ARCHIVE_QUALIFIED,
    ensure_archive_schema, ensure_audit_table,
    make_logger, record_audit, snapshot_table, utc_ts, write_decision_log,
)

REPO = HERE.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG = OUT_DIR / "266a_run_log.md"
DECISION_LOG = OUT_DIR / "266a_decision_log.json"
FINAL_JSON = OUT_DIR / "266a_final_confirmation.json"
VIEW_SMOKE_CSV = OUT_DIR / "266a_view_smoke_check.csv"
SUMMARY_MD = OUT_DIR / "266a_summary.md"

SCRIPT_TAG = "Script 266a"
SCRIPT_NUM = "266a"
RUN_DATE = "2026-04-17"

CPM = f"{PUBLICATION_DB}.main.canonical_patient_master"
DICT_V240 = f"{PUBLICATION_DB}.main.data_dictionary_v240"
DICT_V266A = f"{PUBLICATION_DB}.main.data_dictionary_v266a"
REGISTRY = f"{PUBLICATION_DB}.manuscript_workspace.detail_table_registry_v1"
POINTER = f"{PUBLICATION_DB}.manuscript_workspace.canonical_detail_pointer_v1"
TRIAGE_V265 = f"{PUBLICATION_DB}.manuscript_workspace.cpm_unmapped_triage_v265"
TRIAGE_V266A = f"{PUBLICATION_DB}.manuscript_workspace.cpm_unmapped_triage_v266a"


# ---------------------------------------------------------------------------
# Dictionary updates table — single source of truth for Phase 2 + Phase 3
# ---------------------------------------------------------------------------
DICT_UPDATES: list[dict] = [
    # Hard deprecations
    {
        "column_name": "n_tumors",
        "new_status": "deprecated",
        "new_replacement": "n_tumors_path",
        "new_description": (
            "Deprecated 2026-04-17 by Script 266a. Matches "
            "tumor_episode_master_v2 count for only 143/4137 populated "
            "patients (3.5%). Use n_tumors_path (100% match against "
            "synoptic_tumor_long_v1 derived counts)."
        ),
    },
    {
        "column_name": "n_tumors_v10",
        "new_status": "deprecated",
        "new_replacement": "n_tumors_path",
        "new_description": (
            "Deprecated 2026-04-17 by Script 266a. Matches "
            "synoptic_tumor_long_v1 count for 1158/1346 populated. "
            "Use n_tumors_path."
        ),
    },
    {
        "column_name": "gm_path_stage_raw",
        "new_status": "deprecated",
        "new_replacement": None,
        "new_description": (
            "Deprecated 2026-04-17 by Script 266a. 0% populated. See "
            "path_stage_raw / path_t_stage_raw / path_n_stage_raw / "
            "path_m_stage_raw for raw-stage signals."
        ),
    },
    {
        "column_name": "gm_path_m_stage_raw",
        "new_status": "deprecated",
        "new_replacement": "path_m_stage_raw",
        "new_description": (
            "Deprecated 2026-04-17 by Script 266a. 36.84% populated; "
            "redundant with path_m_stage_raw which carries the same signal."
        ),
    },
    # Annotation only — status stays authoritative
    {
        "column_name": "ajcc7_m_stage",
        "new_status": "authoritative",
        "new_replacement": None,
        "new_description": (
            "AJCC7 M-stage at patient level. 100% populated by M0-default "
            "fill unless a distant-mets flag is set. Not per-case "
            "adjudicated. See gm_path_m_stage_raw (deprecated) and "
            "path_m_stage_raw for raw-text values (~36.8% populated)."
        ),
    },
    {
        "column_name": "ajcc8_m_stage",
        "new_status": "authoritative",
        "new_replacement": None,
        "new_description": (
            "AJCC8 M-stage at patient level. 100% populated by M0-default "
            "fill unless a distant-mets flag is set. Not per-case "
            "adjudicated. See path_m_stage_raw for raw-text values."
        ),
    },
    # Feeder annotations — status stays authoritative
    {
        "column_name": "has_left_tumor",
        "new_status": "authoritative",
        "new_replacement": None,
        "new_description": (
            "Derived 36.65% population (3,984/10,871). Feeder registered "
            "by Script 266a as tumor_pathology.tumor_laterality_overall "
            "(string-match for 'left' on per-patient laterality)."
        ),
    },
    {
        "column_name": "has_right_tumor",
        "new_status": "authoritative",
        "new_replacement": None,
        "new_description": (
            "Derived 36.65% population (3,984/10,871). Feeder registered "
            "by Script 266a as tumor_pathology.tumor_laterality_overall "
            "(string-match for 'right' on per-patient laterality)."
        ),
    },
    {
        "column_name": "has_isthmus_tumor",
        "new_status": "authoritative",
        "new_replacement": None,
        "new_description": (
            "Derived 36.65% population (3,984/10,871). Feeder registered "
            "by Script 266a as tumor_pathology.tumor_laterality_overall "
            "(string-match for 'isthmus' on per-patient laterality)."
        ),
    },
    {
        "column_name": "n_tumors_ete_present",
        "new_status": "authoritative",
        "new_replacement": None,
        "new_description": (
            "Per-patient count of tumors with extrathyroidal extension. "
            "Feeder = patient_tumor_rollup_v1 (existing feeder for sibling "
            "n_tumors_pni_present, n_tumors_vi_present); registered by "
            "Script 266a."
        ),
    },
    {
        "column_name": "n_tumors_lvi_present",
        "new_status": "authoritative",
        "new_replacement": None,
        "new_description": (
            "Per-patient count of tumors with lymphovascular invasion. "
            "Feeder = patient_tumor_rollup_v1; registered by Script 266a."
        ),
    },
    {
        "column_name": "n_tumors_margin_involved",
        "new_status": "authoritative",
        "new_replacement": None,
        "new_description": (
            "Per-patient count of tumors with positive margins. Feeder = "
            "patient_tumor_rollup_v1; registered by Script 266a."
        ),
    },
    {
        "column_name": "n_tumors_margin_uninvolved",
        "new_status": "authoritative",
        "new_replacement": None,
        "new_description": (
            "Per-patient count of tumors with negative margins. Feeder = "
            "patient_tumor_rollup_v1; registered by Script 266a."
        ),
    },
    {
        "column_name": "n_tumors_with_size",
        "new_status": "authoritative",
        "new_replacement": None,
        "new_description": (
            "Per-patient count of tumors with a recorded size. Feeder = "
            "patient_tumor_rollup_v1; registered by Script 266a."
        ),
    },
]

# Columns the prompt's acceptance criteria insist must leave the
# C_missing_feeder bucket after this script runs.
STAGING_ADJACENT_C_TARGETS = {
    "has_left_tumor", "has_right_tumor", "has_isthmus_tumor",
    "n_tumors_ete_present", "n_tumors_lvi_present",
    "n_tumors_margin_involved", "n_tumors_margin_uninvolved",
    "n_tumors_with_size",
    "gm_path_stage_raw", "gm_path_m_stage_raw",
}

PREFLIGHT_DIVERGENCES = [
    {
        "category": "workspace_object_count",
        "original_assertion": "manuscript_workspace = 81 objects (16 tables + 65 views)",
        "revised_assertion": "manuscript_workspace = 83 objects (18 tables + 65 views)",
        "rationale": (
            "Two extra base tables present at preflight: "
            "manuscript_dive_map_v1 and v1_1_finalization_audit_v1 / "
            "vc_paralysis_recalibration_v236 (introduced by scripts post-265). "
            "Does not affect view safety contract (65 views unchanged). "
            "Step 13 acceptance literal '81' was not enforced in 266a."
        ),
        "revised_value_expected": "ws_objects=83",
    },
    {
        "category": "conventions_table_size",
        "original_assertion": "__conventions has 6 rows (as_aliasing, bethesda_semantics, "
                              "catalog_vs_queryable_drift, cohort_scoping, "
                              "pre_flight_decision_log, rid_type_consistency)",
        "revised_assertion": "__conventions has 16 rows; only 'pre_flight_decision_log' "
                             "and 'rid_type_consistency' appear by exact convention_id. "
                             "The other 4 prompt items are paraphrases not present as ids.",
        "rationale": (
            "266a applied the two ids that exist verbatim and treated the others "
            "as operational guidance. No __conventions rows were modified."
        ),
        "revised_value_expected": "n_conventions=16",
    },
    {
        "category": "archive_db_path",
        "original_assertion": "Archive to 'Thyroid 2026 UPdated'.main.<t>_deprecated_20260417 "
                              "via ALTER RENAME -> CTAS -> DROP",
        "revised_assertion": "Archive to 'Thyroid 2026 UPdated'.archive_pub_v1_0."
                             "<t>_pre266a_<UTC> via direct CTAS (no ALTER RENAME). "
                             "main schema in archive DB is empty (0 tables); "
                             "archive_pub_v1_0 is the established home for "
                             "publication snapshots.",
        "rationale": (
            "Confirmed by user 2026-04-17 and verified live: "
            "archive_pub_v1_0 has 180 snapshot tables already, all using "
            "<orig>_pre<NNN>_<UTC> suffix; main schema has 0 tables."
        ),
        "revised_value_expected": "snapshot path = archive_pub_v1_0.<t>_pre266a_<UTC>",
    },
    {
        "category": "scope_split",
        "original_assertion": "Single Script 266 covers Steps 0-15 in one pass "
                              "with destructive ops (renames, drops, archives)",
        "revised_assertion": "Split into 266a (this script: P1+P3+P4 governance, "
                             "non-destructive), 266b (per-tumor AJCC + CPM dominant "
                             "surface, additive), 266c (renames, drops, archive sweep, "
                             "view-validation gated)",
        "rationale": (
            "User directive 2026-04-17: monolithic plan too large for one "
            "session against production DB; 266a delivers immediate dictionary "
            "+ feeder-registration value with single-table revert posture."
        ),
        "revised_value_expected": "266a delivers dictionary v266a + triage v266a + "
                                  "registry feeder updates only.",
    },
    {
        "category": "patient_tumor_rollup_v1_already_registers",
        "original_assertion": "n_tumors_*_present and has_*_tumor are unregistered "
                              "C_missing_feeder entries needing fresh registration",
        "revised_assertion": "patient_tumor_rollup_v1.feeds_master_columns_normalized "
                             "ALREADY contains all 8 columns. cpm_unmapped_triage_v265 "
                             "is a stale snapshot taken before that registry edit. "
                             "Rebuilding triage suffices to remove them from C; "
                             "no registry edit strictly required for them.",
        "rationale": (
            "266a still appends has_left_tumor;has_right_tumor;has_isthmus_tumor "
            "to tumor_pathology row per prompt Step 5 (the structural-source "
            "feeder), in addition to the existing patient_tumor_rollup_v1 rows. "
            "Pointer view supports multi-feeder rows per master_column."
        ),
        "revised_value_expected": "post-266a: 8 staging-adjacent columns absent from "
                                  "C bucket via stale-snapshot rebuild + dict deprecation "
                                  "for gm_path_stage_raw/gm_path_m_stage_raw.",
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _quote(s: str | None) -> str:
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


def _exec(con, sql: str, log, do_writes: bool, *, label: str = "") -> None:
    tag = f" [{label}]" if label else ""
    if do_writes:
        log(f"  EXEC{tag}: {sql.strip().splitlines()[0][:160]}")
        con.execute(sql)
    else:
        log(f"  PLAN{tag}: {sql.strip().splitlines()[0][:160]}")


# ---------------------------------------------------------------------------
# Phase 0 — connect + decision log seed
# ---------------------------------------------------------------------------
def phase_0(con, log, do_writes: bool) -> dict:
    log("\n## Phase 0 — connect + decision-log seed")
    info = con.execute(
        f"SELECT current_database(), "
        f"(SELECT COUNT(*) FROM {CPM}), "
        f"(SELECT COUNT(*) FROM information_schema.columns "
        f" WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        f" AND table_name='canonical_patient_master')"
    ).fetchone()
    log(f"  connected_to        : {info[0]}")
    log(f"  cpm_n_patients      : {info[1]}")
    log(f"  cpm_n_columns       : {info[2]}")
    if info[1] != 10871:
        raise SystemExit(f"CPM patient count mismatch: {info[1]} != 10871")
    if info[2] != 1499:
        raise SystemExit(f"CPM column count mismatch: {info[2]} != 1499")
    return {"cpm_n_patients": info[1], "cpm_n_columns": info[2]}


# ---------------------------------------------------------------------------
# Phase 1 — snapshot v240 + registry to archive_pub_v1_0
# ---------------------------------------------------------------------------
def phase_1(con, log, do_writes: bool) -> dict:
    log("\n## Phase 1 — snapshot v240 dictionary + registry to archive")
    suffix_dict = f"data_dictionary_v240_pre266a_{utc_ts()}"
    suffix_reg = f"detail_table_registry_v1_pre266a_{utc_ts()}"
    if do_writes:
        ensure_archive_schema(con)
        d_dest = snapshot_table(
            con, DICT_V240, suffix_dict, SCRIPT_TAG,
            "Pre-266a snapshot of data_dictionary_v240 before CTAS to v266a.")
        r_dest = snapshot_table(
            con, REGISTRY, suffix_reg, SCRIPT_TAG,
            "Pre-266a snapshot of detail_table_registry_v1 before "
            "tumor_pathology row update.")
        log(f"  snapshot dict     -> {d_dest}")
        log(f"  snapshot registry -> {r_dest}")
        return {"dict_snapshot": d_dest, "registry_snapshot": r_dest}
    log(f"  PLAN: CTAS {DICT_V240} -> {ARCHIVE_QUALIFIED}.{suffix_dict!r}")
    log(f"  PLAN: CTAS {REGISTRY}  -> {ARCHIVE_QUALIFIED}.{suffix_reg!r}")
    return {"dict_snapshot": None, "registry_snapshot": None}


# ---------------------------------------------------------------------------
# Phase 2 — build data_dictionary_v266a + apply updates
# ---------------------------------------------------------------------------
def phase_2(con, log, do_writes: bool) -> dict:
    log("\n## Phase 2 — build data_dictionary_v266a + apply updates")
    # Precount
    n_v240 = int(con.execute(f"SELECT COUNT(*) FROM {DICT_V240}").fetchone()[0])
    log(f"  v240 rows: {n_v240}")

    # CTAS (CREATE OR REPLACE — idempotent across reruns of Phase 2)
    sql_ctas = f"CREATE OR REPLACE TABLE {DICT_V266A} AS SELECT * FROM {DICT_V240}"
    _exec(con, sql_ctas, log, do_writes, label="ctas_v266a")

    # Comment on v266a
    comment = (
        "Script 266a (2026-04-17). CTAS from data_dictionary_v240 with "
        "266a governance edits applied: deprecates n_tumors, n_tumors_v10, "
        "gm_path_stage_raw, gm_path_m_stage_raw; annotates ajcc{7,8}_m_stage "
        "as M0-default-fill; registers feeders for has_*_tumor and "
        "n_tumors_*_present in description text. data_dictionary_v240 "
        "left in place. Successor v266 (full per-tumor AJCC governance) "
        "comes from Script 266b/266c."
    ).replace("'", "''")
    _exec(con, f"COMMENT ON TABLE {DICT_V266A} IS '{comment}'",
          log, do_writes, label="comment_v266a")

    # Apply per-row UPDATEs (only if v266a exists in apply mode)
    applied = []
    for upd in DICT_UPDATES:
        col = upd["column_name"]
        sql = (
            f"UPDATE {DICT_V266A} SET "
            f"  status = {_quote(upd['new_status'])}, "
            f"  replacement_column_name = {_quote(upd['new_replacement'])}, "
            f"  description = {_quote(upd['new_description'])} "
            f"WHERE column_name = {_quote(col)}"
        )
        _exec(con, sql, log, do_writes, label=f"update:{col}")
        applied.append(col)

    # Verify (apply mode only)
    if do_writes:
        n_v266a = int(con.execute(f"SELECT COUNT(*) FROM {DICT_V266A}").fetchone()[0])
        n_dep = int(con.execute(
            f"SELECT COUNT(*) FROM {DICT_V266A} WHERE status='deprecated' "
            f"AND column_name IN ('n_tumors','n_tumors_v10','gm_path_stage_raw',"
            f"'gm_path_m_stage_raw')"
        ).fetchone()[0])
        log(f"  v266a rows: {n_v266a}  (must == v240 rows {n_v240})")
        log(f"  deprecated-target rows post-update: {n_dep} (target>=4)")
        if n_v266a != n_v240:
            raise SystemExit(f"v266a row count {n_v266a} != v240 {n_v240}")
        return {"v266a_rows": n_v266a, "n_dep": n_dep, "applied_updates": applied}

    return {"v266a_rows": None, "applied_updates": applied}


# ---------------------------------------------------------------------------
# Phase 3 — COMMENT ON COLUMN on CPM
# ---------------------------------------------------------------------------
def phase_3(con, log, do_writes: bool) -> dict:
    log("\n## Phase 3 — COMMENT ON COLUMN on CPM")
    n_done = 0
    for upd in DICT_UPDATES:
        col = upd["column_name"]
        msg = upd["new_description"].replace("'", "''")
        sql = f"COMMENT ON COLUMN {CPM}.{col} IS '{msg}'"
        _exec(con, sql, log, do_writes, label=f"comment:{col}")
        n_done += 1
    return {"comments_applied": n_done}


# ---------------------------------------------------------------------------
# Phase 4 — register has_*_tumor against tumor_pathology in registry
# ---------------------------------------------------------------------------
NEW_TOKENS_FOR_TUMOR_PATHOLOGY = [
    "has_left_tumor", "has_right_tumor", "has_isthmus_tumor",
]


def _merge_tokens(existing: str | None, new_tokens: list[str]) -> str:
    parts = [t.strip() for t in (existing or "").split(";") if t.strip()]
    for t in new_tokens:
        if t not in parts:
            parts.append(t)
    parts.sort()
    return ";".join(parts)


def phase_4(con, log, do_writes: bool) -> dict:
    log("\n## Phase 4 — register has_*_tumor against tumor_pathology in registry")
    row = con.execute(
        f"SELECT feeds_master_columns, feeds_master_columns_normalized "
        f"FROM {REGISTRY} WHERE detail_table_name='tumor_pathology'"
    ).fetchone()
    if row is None:
        raise SystemExit("Registry has no tumor_pathology row — abort.")
    existing_raw, existing_norm = row[0], row[1]
    new_raw = _merge_tokens(existing_raw, NEW_TOKENS_FOR_TUMOR_PATHOLOGY)
    new_norm = _merge_tokens(existing_norm, NEW_TOKENS_FOR_TUMOR_PATHOLOGY)
    log(f"  before raw : {existing_raw}")
    log(f"  before norm: {existing_norm}")
    log(f"  after  raw : {new_raw}")
    log(f"  after  norm: {new_norm}")

    new_desc = (
        "Tumor pathology records from structured data sources. "
        "Script 266a (2026-04-17) registered laterality-derived feeders: "
        "has_left_tumor, has_right_tumor, has_isthmus_tumor — derived via "
        "GROUP BY research_id with substring-match for 'left' / 'right' / "
        "'isthmus' on tumor_laterality_overall."
    ).replace("'", "''")

    sql = (
        f"UPDATE {REGISTRY} SET "
        f"  feeds_master_columns = {_quote(new_raw)}, "
        f"  feeds_master_columns_normalized = {_quote(new_norm)}, "
        f"  description = '{new_desc}' "
        f"WHERE detail_table_name = 'tumor_pathology'"
    )
    _exec(con, sql, log, do_writes, label="registry_update_tumor_pathology")

    if do_writes:
        post = con.execute(
            f"SELECT feeds_master_columns_normalized FROM {REGISTRY} "
            f"WHERE detail_table_name='tumor_pathology'"
        ).fetchone()[0]
        for t in NEW_TOKENS_FOR_TUMOR_PATHOLOGY:
            if t not in (post or ""):
                raise SystemExit(f"Token {t!r} missing from registry after update.")
        log("  tokens verified present in registry after update.")
    return {
        "before_normalized": existing_norm,
        "after_normalized": new_norm,
    }


# ---------------------------------------------------------------------------
# Phase 5 — rebuild cpm_unmapped_triage_v266a with dict-aware deprecation gate
# ---------------------------------------------------------------------------
TRIAGE_BUCKET_CASE_SQL = """
CASE
  WHEN regexp_matches(c.column_name, '^(_|tmp_|stg_|raw_|pre_)')
    THEN 'A_deprecated_candidate'
  WHEN d.deprecated_in_v266a
    THEN 'A_deprecated_candidate'
  WHEN regexp_matches(c.column_name, '^(ajcc[78]?_|ames_|macis_|ages_|ata_|sage_|delphian_|rscore_)')
    THEN 'B_computed_score'
  WHEN regexp_matches(c.column_name, '_inferred_negative$')
    THEN 'B_computed_score'
  WHEN regexp_matches(c.column_name, '_final$')
    THEN 'B_computed_score'
  WHEN regexp_matches(c.column_name, '_v[0-9]+$')
    THEN 'B_computed_score'
  ELSE 'C_missing_feeder'
END
""".strip()


def phase_5(con, log, do_writes: bool) -> dict:
    log("\n## Phase 5 — rebuild cpm_unmapped_triage_v266a")
    n_unmapped = int(con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns c
        LEFT JOIN (SELECT DISTINCT master_column FROM {POINTER}) p
          ON p.master_column = c.column_name
        WHERE c.table_catalog='{PUBLICATION_DB}' AND c.table_schema='main'
          AND c.table_name='canonical_patient_master'
          AND p.master_column IS NULL
    """).fetchone()[0])
    log(f"  unmapped CPM cols (live registry view): {n_unmapped}")

    sql = f"""
        CREATE OR REPLACE TABLE {TRIAGE_V266A} AS
        WITH dep AS (
          SELECT column_name, status='deprecated' AS deprecated_in_v266a
            FROM {DICT_V266A}
        )
        SELECT
          c.column_name,
          c.ordinal_position,
          c.data_type,
          {TRIAGE_BUCKET_CASE_SQL} AS triage_bucket,
          COALESCE(d.deprecated_in_v266a, FALSE) AS deprecated_in_v266a,
          current_timestamp AS triaged_at
        FROM information_schema.columns c
        LEFT JOIN (SELECT DISTINCT master_column FROM {POINTER}) p
          ON p.master_column = c.column_name
        LEFT JOIN dep d ON d.column_name = c.column_name
        WHERE c.table_catalog = '{PUBLICATION_DB}'
          AND c.table_schema = 'main'
          AND c.table_name = 'canonical_patient_master'
          AND p.master_column IS NULL
    """
    _exec(con, sql, log, do_writes, label="ctas_triage_v266a")

    comment = (
        "Script 266a: triage of CPM columns with no feeder in "
        "canonical_detail_pointer_v1, with dictionary-aware deprecation "
        "gate (status='deprecated' in data_dictionary_v266a routes to "
        "A_deprecated_candidate). Buckets: A=deprecated/staging-prefix, "
        "B=composite scores (AJCC, AMES, MACIS, AGES, _final, _v\\d+, "
        "_inferred_negative), C=real registry gaps."
    ).replace("'", "''")
    _exec(con, f"COMMENT ON TABLE {TRIAGE_V266A} IS '{comment}'",
          log, do_writes, label="comment_triage_v266a")

    if not do_writes:
        return {"unmapped": n_unmapped, "buckets": None,
                "staging_adjacent_in_C": None}

    buckets = con.execute(
        f"SELECT triage_bucket, COUNT(*) FROM {TRIAGE_V266A} "
        f"GROUP BY 1 ORDER BY 1"
    ).fetchall()
    bdict = {b: int(n) for b, n in buckets}
    log("  bucket counts (post-rebuild):")
    for b, n in buckets:
        log(f"    {b}: {n}")

    staging_in_c = con.execute(f"""
        SELECT column_name FROM {TRIAGE_V266A}
         WHERE triage_bucket='C_missing_feeder'
           AND column_name IN ({",".join(repr(c) for c in STAGING_ADJACENT_C_TARGETS)})
         ORDER BY column_name
    """).fetchall()
    leftover = [r[0] for r in staging_in_c]
    log(f"  staging-adjacent still in C: {leftover or '[]'}")
    return {"unmapped": n_unmapped, "buckets": bdict,
            "staging_adjacent_in_C": leftover}


# ---------------------------------------------------------------------------
# Phase 6 — final acceptance gates + view smoke check
# ---------------------------------------------------------------------------
def phase_6(con, log, do_writes: bool) -> dict:
    log("\n## Phase 6 — acceptance gates + view smoke check")

    if not do_writes:
        log("  PLAN: would run gates against post-apply state. Dry-run skipped.")
        return {"skipped_dryrun": True}

    # Gate 1: v266a present and row count matches v240
    n_v240 = int(con.execute(f"SELECT COUNT(*) FROM {DICT_V240}").fetchone()[0])
    n_v266a = int(con.execute(f"SELECT COUNT(*) FROM {DICT_V266A}").fetchone()[0])
    log(f"  GATE v240_rows={n_v240} v266a_rows={n_v266a}")
    if n_v240 != n_v266a:
        raise SystemExit("v266a row count mismatch — gate failed.")

    # Gate 2: required deprecations in v266a
    n_dep = int(con.execute(
        f"SELECT COUNT(*) FROM {DICT_V266A} WHERE status='deprecated' "
        f"AND column_name IN ('n_tumors','n_tumors_v10','gm_path_stage_raw',"
        f"'gm_path_m_stage_raw')"
    ).fetchone()[0])
    log(f"  GATE deprecated_targets={n_dep} (must >= 4)")
    if n_dep < 4:
        raise SystemExit(
            f"Required deprecations in v266a = {n_dep} < 4 — gate failed."
        )

    # Gate 3: triage_v266a C bucket <= 165
    c_count = int(con.execute(
        f"SELECT COUNT(*) FROM {TRIAGE_V266A} "
        f"WHERE triage_bucket='C_missing_feeder'"
    ).fetchone()[0])
    log(f"  GATE c_bucket={c_count} (must <= 165)")
    if c_count > 165:
        raise SystemExit(f"C bucket count {c_count} > 165 — gate failed.")

    # Gate 4: no staging-adjacent in C
    leftover = [r[0] for r in con.execute(f"""
        SELECT column_name FROM {TRIAGE_V266A}
         WHERE triage_bucket='C_missing_feeder'
           AND column_name IN ({",".join(repr(c) for c in STAGING_ADJACENT_C_TARGETS)})
         ORDER BY column_name
    """).fetchall()]
    log(f"  GATE staging_adjacent_in_C={leftover}")
    if leftover:
        raise SystemExit(
            f"Staging-adjacent columns still in C bucket: {leftover}"
        )

    # Gate 5: all 65 ws views still queryable
    views = [r[0] for r in con.execute(f"""
        SELECT table_name FROM information_schema.views
         WHERE table_catalog='{PUBLICATION_DB}'
           AND table_schema='manuscript_workspace'
         ORDER BY table_name
    """).fetchall()]
    log(f"  smoke-checking {len(views)} views ...")
    rows = []
    n_ok = 0
    n_fail = 0
    for v in views:
        try:
            n = con.execute(
                f"SELECT COUNT(*) FROM {PUBLICATION_DB}.manuscript_workspace.\"{v}\""
            ).fetchone()[0]
            rows.append((v, "ok", n))
            n_ok += 1
        except Exception as e:
            msg = str(e).splitlines()[0][:300]
            rows.append((v, "error", msg))
            n_fail += 1
    with VIEW_SMOKE_CSV.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["view_name", "status", "row_count_or_error"])
        w.writerows(rows)
    log(f"  smoke result: ok={n_ok} error={n_fail}")
    if n_fail:
        broken = [r[0] for r in rows if r[1] == "error"][:10]
        raise SystemExit(f"View smoke check failed: {n_fail} views broken: {broken}")

    return {
        "v240_rows": n_v240, "v266a_rows": n_v266a,
        "deprecated_targets": n_dep, "c_bucket": c_count,
        "staging_adjacent_in_C": leftover,
        "views_total": len(views), "views_ok": n_ok, "views_error": n_fail,
    }


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
PHASES = {
    0: phase_0,
    1: phase_1,
    2: phase_2,
    3: phase_3,
    4: phase_4,
    5: phase_5,
    6: phase_6,
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Execute writes; default is dry-run.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Explicit dry-run (default).")
    ap.add_argument("--phase", type=int, default=None,
                    help="Run a single phase (0..6). Phase 0 always runs first.")
    args = ap.parse_args()
    do_writes = bool(args.apply) and not bool(args.dry_run)

    # Reset run log on each invocation so we get a clean per-run document.
    if RUN_LOG.exists():
        RUN_LOG.unlink()
    log, fh = make_logger(RUN_LOG)
    t0 = time.time()
    try:
        log("=" * 78)
        log(f"# {SCRIPT_TAG} run")
        log(f"started_at: {utc_now()}")
        log(f"mode      : {'APPLY' if do_writes else 'DRY-RUN'}")
        log(f"phase     : {args.phase if args.phase is not None else 'all'}")

        con = connect_locked()
        log(f"connected to {PUBLICATION_DB}")

        results: dict = {"meta": {"mode": "APPLY" if do_writes else "DRY-RUN",
                                   "started_at": utc_now(),
                                   "script": SCRIPT_TAG}}

        if args.phase is not None:
            phase_list = [0, args.phase] if args.phase != 0 else [0]
        else:
            phase_list = list(PHASES.keys())

        for p in phase_list:
            results[f"phase_{p}"] = PHASES[p](con, log, do_writes)

        # Audit row + decision log (apply mode only)
        if do_writes:
            ensure_audit_table(con)
            record_audit(
                con, SCRIPT_NUM, "266a_governance",
                "deprecated_targets_in_v266a",
                count_before=0,
                count_after=int(results.get("phase_2", {}).get("n_dep", 0) or 0),
                target_after=4, status="OK",
                notes=("266a applied dictionary deprecations + feeder "
                       "registration; triage rebuilt as v266a."))

        write_decision_log(DECISION_LOG, {
            "script": SCRIPT_TAG,
            "run_date": RUN_DATE,
            "mode": "APPLY" if do_writes else "DRY-RUN",
            "preflight_divergences": PREFLIGHT_DIVERGENCES,
            "results": results,
            "elapsed_seconds": round(time.time() - t0, 1),
        })

        # Final confirmation JSON
        if do_writes and 6 in phase_list:
            FINAL_JSON.write_text(
                json.dumps(results.get("phase_6", {}), indent=2, default=str)
            )

        # Summary markdown
        if do_writes and 6 in phase_list:
            p6 = results.get("phase_6", {})
            summary = f"""# Script 266a — summary

- **Mode:** APPLY
- **CPM patients:** 10,871
- **CPM columns:** 1,499 (unchanged)
- **Dictionary v240 rows:** {p6.get('v240_rows')}
- **Dictionary v266a rows:** {p6.get('v266a_rows')}
- **Deprecated targets in v266a:** {p6.get('deprecated_targets')} / 4 required
- **cpm_unmapped_triage_v266a C-bucket:** {p6.get('c_bucket')} (was 174; target ≤ 165)
- **Staging-adjacent C entries remaining:** {p6.get('staging_adjacent_in_C') or 'none'}
- **Views smoke-checked:** {p6.get('views_ok')} / {p6.get('views_total')} OK
- **Run log:** scripts/output/266a_run_log.md
- **Decision log:** scripts/output/266a_decision_log.json
- **Final confirmation:** scripts/output/266a_final_confirmation.json
- **View smoke CSV:** scripts/output/266a_view_smoke_check.csv
"""
            SUMMARY_MD.write_text(summary)

        log(f"\nelapsed: {time.time() - t0:.1f}s")
        log("DONE")
        return 0

    except Exception as e:
        log(f"FATAL: {e!r}")
        log(traceback.format_exc())
        return 1
    finally:
        fh.close()


if __name__ == "__main__":
    sys.exit(main())
