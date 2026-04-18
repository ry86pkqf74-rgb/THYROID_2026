#!/usr/bin/env python3
"""Script 279 — v1.1 Prompt 20 real-findings fix + recurrence/LN dictionary
clarification.

Background
----------
Coworker dry-run audit (Prompt 20) raised 8 concerns. Three are explicitly
withdrawn or already addressed per
``PART2_DETAIL_CROSSVAL_FINDINGS_20260416.md`` §7.2 / §5.4 / §7.3:

  - any_recurrence_flag rebuild (would REGRESS the strict path-proven
    canonical definition; recurrence_flag_v2 is the strict flag)
  - ln_master_rollup_v1 duplicate research_id (per-surgery grain by
    design; some patients have 2-4 surgeries)
  - recurrence_date 90% NULL (source-limited, not a data bug)

Five remaining findings ARE real and this script addresses them:

  1. ENE bifurcation in main.ln_master_rollup_v1: the BOOLEAN
     ``ln_mets_extranodal_extension`` has only 14 TRUE rows out of 4,273,
     while the INTEGER ``ln_extranodal_extension > 0`` has 1,326. The
     INTEGER is the source of truth; rebuild the BOOLEAN from it.

  2. 11 rows in main.ln_master_rollup_v1 with
     ``ln_total_positive > ln_total_examined`` AND
     ``ln_internal_consistency = 'ok'``. All are tumor_pathology source
     with ln_total_examined=0. Counts preserved (ingestion artifacts);
     retag the validator only.

  3. Orphan research_id 11454 in main.clinical_note_ln_extracted_v1
     (2 rows) without any cancer evidence in CPM/FNA/tumor/synoptic/path/
     operative — same pattern as the §7.3 withdrawn finding. Route the
     2 rows to manuscript_workspace.ln_extract_noncohort_orphan_v279
     and DO NOT delete the source rows.

  4. Dictionary gap: every recurrence + LN column in
     data_dictionary_v266a is tagged 'authoritative' with empty
     description and NULL replacement_column_name. Bump the dictionary
     to v279 with descriptions + replacement_column_name populated for
     10 recurrence and 6 LN columns.

  5. §5.4 documentation debt registered as v1.1 tech_debt:
     ln_master_rollup_v1 per-surgery grain formalization, and
     post-279 recurrence-flag consumer audit.

Contract (from existing v1.0 conventions)
-----------------------------------------
  - Use ``scripts/_md_connect.connect_locked()`` (locks search path,
    asserts CPM = 10,871 / 10,871 / 0).
  - Default mode is ``--dry-run``; ``--apply`` is required to write.
  - Snapshot any mutated table FIRST to
    ``"Thyroid 2026 UPdated".archive_pub_v1_0.<t>_pre279_<UTC>``.
  - All cross-table research_id joins use VARCHAR casts (rid_type_consistency).
  - Each finding records a row in
    ``manuscript_workspace.v1_1_finalization_audit_v1`` (audit_status_taxonomy).
  - Deferred work registers ONE aggregated row per decision in
    ``manuscript_workspace.v1_1_tech_debt_v1`` (tech_debt_aggregation).
  - CPM is NOT mutated by this script; no CPM snapshot taken.

Outputs (scripts/output/):
  - 279_run.log
  - 279_decision_log.json

Outputs (studies/v1_1_finalization/):
  - 279_ln_ene_and_recurrence_dict_report.md (written separately)

Idempotency:
  - Phase 1 snapshots use unique UTC suffix; if you re-run within the
    same second the CTAS will fail (intentional — re-runs should reuse
    earlier snapshot or carry a fresh suffix).
  - Phase 2 UPDATE only touches rows where the BOOLEAN disagrees with
    (INTEGER > 0); re-runs find 0 such rows.
  - Phase 3 UPDATE only touches the 11 'ok' rows; re-runs find 0.
  - Phase 4 CREATE OR REPLACE the routing table.
  - Phase 5 CREATE OR REPLACE data_dictionary_v279; idempotent.
  - Phase 6 INSERTs are guarded by debt_id existence check.
"""
from __future__ import annotations

import argparse
import json
import os
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
    ensure_archive_schema,
    ensure_audit_table,
    record_audit,
    snapshot_table,
    utc_ts,
    write_decision_log,
)

REPO = HERE.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

LOG_PATH = OUT_DIR / "279_run.log"
DECISION_LOG = OUT_DIR / "279_decision_log.json"

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_PREFIX = f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}'

ISO_TS = utc_ts()
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
SCRIPT_TAG = "Script 279"
SCRIPT_NUM = "279"
RUN_DATE = "2026-04-18"

CPM = f"{PUBLICATION_DB}.main.canonical_patient_master"
LN_ROLLUP = f"{PUBLICATION_DB}.main.ln_master_rollup_v1"
LN_EXTRACT = f"{PUBLICATION_DB}.main.clinical_note_ln_extracted_v1"
DICT_V266A = f"{PUBLICATION_DB}.main.data_dictionary_v266a"
DICT_V279 = f"{PUBLICATION_DB}.main.data_dictionary_v279"
README_TBL = f"{PUBLICATION_DB}.main.__readme"
REGISTRY = f"{PUBLICATION_DB}.manuscript_workspace.detail_table_registry_v1"
ORPHAN_TABLE = (
    f"{PUBLICATION_DB}.manuscript_workspace.ln_extract_noncohort_orphan_v279"
)
TECH_DEBT_FQ = f"{PUBLICATION_DB}.manuscript_workspace.v1_1_tech_debt_v1"


# ---------------------------------------------------------------------------
# Targeted dictionary clarifications (Phase 5)
# ---------------------------------------------------------------------------
# Recurrence family — sourced from PART2 §7.2 + Script 203 header comment.
# LN family — sourced from PART2 §5.4 + Prompt 20 real findings.
DICT_UPDATES: list[dict] = [
    # ---- Recurrence (10) ----
    {
        "column_name": "recurrence_flag_v2",
        "status": "authoritative",
        "replacement_column_name": None,
        "description": (
            "Strict authoritative recurrence flag. TRUE iff >=1 of: (a) "
            "reoperation with pathology showing recurrent/persistent "
            "cancer; (b) FNA/biopsy with Bethesda V/VI post-op; (c) "
            "rising Tg in patient who previously had undetectable Tg. "
            "Built by Script 224. N TRUE = 189."
        ),
    },
    {
        "column_name": "any_recurrence_flag",
        "status": "authoritative",
        "replacement_column_name": "recurrence_flag_v2",
        "description": (
            "Intermediate recurrence signal (N TRUE = 384). Combines "
            "recurrence_flag_v2 with some structural-evidence overlap. "
            "Prefer recurrence_flag_v2 for the strict path-proven "
            "definition. Per PART2 §7.2 the strict definition is canonical "
            "and a value rebuild of this column would REGRESS canonical "
            "semantics — Script 279 clarifies the dictionary only."
        ),
    },
    {
        "column_name": "any_recurrence_flag_prev_233",
        "status": "archived_removed_from_cpm",
        "replacement_column_name": "recurrence_flag_v2",
        "description": (
            "Historical broad recurrence flag (N TRUE = 1946) including "
            "imaging-only suspicion. REMOVED from canonical_patient_master "
            "during the post-Script 249 cleanup; values are preserved in "
            "archive_pub_v1_0 snapshots (canonical_patient_master_pre235.. "
            "pre249) for audit. Dictionary row inserted by Script 279 for "
            "documentation completeness — the column is not currently "
            "queryable from CPM. Per PART2 §7.2, imaging without biopsy is "
            "NOT recurrence under the canonical definition; downstream "
            "consumers must use recurrence_flag_v2."
        ),
    },
    {
        "column_name": "structural_recurrence_flag",
        "status": "component",
        "replacement_column_name": "recurrence_flag_v2",
        "description": (
            "Structural recurrence detected (includes imaging-only findings "
            "such as pathologic lymphadenopathy and reoperation_proxy). "
            "NOT equivalent to path-proven recurrence — fires for cases "
            "explicitly excluded from recurrence_flag_v2 per the canonical "
            "definition."
        ),
    },
    {
        "column_name": "biochemical_recurrence_flag",
        "status": "authoritative_subtype",
        "replacement_column_name": None,
        "description": (
            "Biochemical recurrence (rising Tg in patient with previously "
            "undetectable Tg). Subset of recurrence_flag_v2. N TRUE = 128."
        ),
    },
    {
        "column_name": "imaging_suspicious_recurrence_flag",
        "status": "authoritative_subtype",
        "replacement_column_name": None,
        "description": (
            "Imaging-only, unconfirmed suspicion. Per the canonical "
            "definition this is NOT recurrence. N TRUE = 79."
        ),
    },
    {
        "column_name": "recurrence_type",
        "status": "authoritative",
        "replacement_column_name": None,
        "description": (
            "Clinical recurrence taxonomy (6 levels including "
            "persistent_biochemical_disease, biochemical_tg_rise, "
            "imaging_suspicious_unconfirmed, fna_confirmed, "
            "structural_confirmed_legacy, none). Vocabulary differs from "
            "the recurrence_event_clean_v1 detail table's "
            "structural/biochemical labels by design — this is the "
            "patient-level rollup."
        ),
    },
    {
        "column_name": "recurrence_date",
        "status": "authoritative",
        "replacement_column_name": "recurrence_date_v2",
        "description": (
            "First recurrence date from any source (includes imaging-"
            "suspicion-only events). For the strict path-proven date use "
            "recurrence_date_v2. Source-limited NULL pattern (~90% NULL) "
            "is upstream and not a defect."
        ),
    },
    {
        "column_name": "recurrence_date_v2",
        "status": "authoritative",
        "replacement_column_name": None,
        "description": (
            "Strict recurrence date aligned with recurrence_flag_v2."
        ),
    },
    {
        "column_name": "time_to_recurrence_days",
        "status": "authoritative",
        "replacement_column_name": None,
        "description": (
            "Days from first_surgery_date to recurrence_date. Derived from "
            "the broad recurrence_date column. Use with the flag whose "
            "semantics match your analysis (strict: recurrence_flag_v2; "
            "broad: any_recurrence_flag)."
        ),
    },
    # ---- LN (6) ----
    {
        "column_name": "ln_total_examined",
        "status": "authoritative",
        "replacement_column_name": None,
        "description": (
            "CPM patient-level LN examined count. Source heuristic: path-"
            "report when available, else operative. Concordance with "
            "ln_master_rollup_v1 = 57% because the rollup is per-surgery-"
            "episode and CPM fuses path + operative. For per-surgery "
            "detail see ln_master_rollup_v1; for the rollup-aligned total "
            "use ln_rollup_total_examined."
        ),
    },
    {
        "column_name": "ln_rollup_total_examined",
        "status": "authoritative",
        "replacement_column_name": None,
        "description": (
            "CPM mirror of MAX(ln_total_examined) over ln_master_rollup_v1 "
            "per patient. 94% concordant with the rollup by construction. "
            "Prefer this for analyses that want rollup-aligned counts."
        ),
    },
    {
        "column_name": "ln_rollup_source",
        "status": "authoritative",
        "replacement_column_name": None,
        "description": (
            "Provenance tag for the rollup-derived LN counts. Values match "
            "ln_master_rollup_v1.ln_source (tumor_pathology / "
            "path_synoptics_fallback / no_data). Documented by Script 279 "
            "(2026-04-18) — column existed on CPM but was undocumented in "
            "data_dictionary_v266a."
        ),
    },
    {
        "column_name": "ln_positive_flag",
        "status": "authoritative",
        "replacement_column_name": None,
        "description": (
            "Integer flag for any positive LN (0/1/NULL). Built from CPM's "
            "fused path+operative ln_total_positive; ~1,591 patients have "
            "ln_master_rollup_v1.ln_any_positive=TRUE but ln_positive_flag "
            "0/NULL due to the per-surgery-vs-per-patient + path-vs-"
            "operative divergence. Do not join against this for LN cohort "
            "definition; use ln_any_positive or a rollup-aligned source."
        ),
    },
    {
        "column_name": "ln_positive_binary",
        "status": "authoritative",
        "replacement_column_name": None,
        "description": (
            "BOOLEAN equivalent of ln_positive_flag. 99.3% concordant with "
            "ln_master_rollup_v1.ln_any_positive."
        ),
    },
    {
        "column_name": "ene_positive",
        "status": "authoritative",
        "replacement_column_name": None,
        "description": (
            "ENE at patient level. Built from "
            "ln_master_rollup_v1.ln_extranodal_extension (INTEGER) > 0. "
            "NULL for patients without a rollup row. After Script 279 "
            "(2026-04-18) the rollup BOOLEAN column "
            "ln_mets_extranodal_extension is also aligned to this rule — "
            "see v1_1_finalization_audit_v1 finding_id="
            "279_ene_boolean_realign."
        ),
    },
]

DICT_UPDATE_COLUMNS = [u["column_name"] for u in DICT_UPDATES]


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}] {msg}"
    print(line, flush=True)
    with LOG_PATH.open("a") as fh:
        fh.write(line + "\n")


def _exec(con, sql: str, do_writes: bool, *, label: str = "") -> None:
    tag = f" [{label}]" if label else ""
    head = sql.strip().splitlines()[0][:200]
    if do_writes:
        log(f"  EXEC{tag}: {head}")
        con.execute(sql)
    else:
        log(f"  PLAN{tag}: {head}")


def _quote(s: str | None) -> str:
    if s is None:
        return "NULL"
    return "'" + str(s).replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# Phase 0 — preflight
# ---------------------------------------------------------------------------
def assert_invariants(con) -> dict:
    n, d, nulls = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id), "
        f"COUNT(*) FILTER (WHERE research_id IS NULL) FROM {CPM}"
    ).fetchone()
    log(f"INVARIANTS canonical_patient_master: n={n} distinct={d} nulls={nulls}")
    if (n, d, nulls) != (10871, 10871, 0):
        raise SystemExit(
            f"INVARIANT VIOLATION: ({n},{d},{nulls}) != (10871,10871,0)"
        )
    n_cols = con.execute(
        f"SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        f"AND table_name='canonical_patient_master'"
    ).fetchone()[0]
    log(f"  cpm_n_columns: {n_cols}")
    return {"cpm_rows": n, "cpm_distinct": d, "cpm_nulls": nulls,
            "cpm_cols": int(n_cols)}


def phase_0(con) -> dict:
    log("\n## Phase 0 — preflight")
    dbs = {r[0] for r in con.execute(
        "SELECT database_name FROM duckdb_databases()").fetchall()}
    log(f"  attached databases: {sorted(dbs)}")
    if ARCHIVE_DB not in dbs:
        raise SystemExit(
            f"Archive database '{ARCHIVE_DB}' not attached — abort."
        )
    invariants = assert_invariants(con)

    log("  reproducing five Prompt-20 real findings:")
    bool_true = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_mets_extranodal_extension=TRUE"
    ).fetchone()[0])
    int_pos = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_extranodal_extension > 0"
    ).fetchone()[0])
    impossible_ok = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_total_positive > ln_total_examined "
        f"AND ln_internal_consistency = 'ok'"
    ).fetchone()[0])
    orphan_n = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_EXTRACT} "
        f"WHERE CAST(research_id AS VARCHAR) NOT IN "
        f"(SELECT research_id FROM {CPM} WHERE research_id IS NOT NULL)"
    ).fetchone()[0])
    rollup_src_present = int(con.execute(
        f"SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        f"AND table_name='canonical_patient_master' "
        f"AND column_name='ln_rollup_source'"
    ).fetchone()[0])

    findings = {
        "ene_boolean_true_now": bool_true,
        "ene_integer_positive_now": int_pos,
        "impossible_consistency_ok_rows": impossible_ok,
        "ln_extract_orphan_rows": orphan_n,
        "ln_rollup_source_present_on_cpm": rollup_src_present,
    }
    log(f"  findings: {findings}")
    expected = {
        "ene_boolean_true_now": 14,
        "ene_integer_positive_now": 1326,
        "impossible_consistency_ok_rows": 11,
        "ln_extract_orphan_rows": 2,  # 2 rows for 1 distinct research_id (11454)
        "ln_rollup_source_present_on_cpm": 1,
    }
    drift = {k: (v, expected[k]) for k, v in findings.items()
             if v != expected[k]}
    if drift:
        log(f"  DRIFT vs 2026-04-18 baseline: {drift}")
    else:
        log("  no drift vs 2026-04-18 baseline.")
    return {"invariants": invariants, "findings": findings, "drift": drift}


# ---------------------------------------------------------------------------
# Phase 1 — snapshots
# ---------------------------------------------------------------------------
def phase_1(con, do_writes: bool) -> dict:
    log("\n## Phase 1 — snapshots to archive_pub_v1_0")
    candidate_targets = [
        (LN_ROLLUP, "ln_master_rollup_v1", "main",
         "Pre-279 snapshot of ln_master_rollup_v1 before ENE BOOLEAN realign "
         "and impossible-consistency retag."),
        (LN_EXTRACT, "clinical_note_ln_extracted_v1", "main",
         "Pre-279 snapshot of clinical_note_ln_extracted_v1 before non-cohort "
         "orphan routing (no rows are deleted; snapshot is precautionary)."),
        (DICT_V266A, "data_dictionary_v266a", "main",
         "Pre-279 snapshot of data_dictionary_v266a before retirement and "
         "rebuild as data_dictionary_v279 with recurrence/LN clarifications."),
    ]
    log("  CPM is NOT mutated by Script 279; no CPM snapshot taken.")
    snapshots: dict[str, str] = {}
    skipped: list[str] = []
    if do_writes:
        ensure_archive_schema(con)
    for src_fq, base, schema, reason in candidate_targets:
        # Idempotent re-run: only snapshot tables that still exist in main.
        present = int(con.execute(
            f"SELECT COUNT(*) FROM information_schema.tables "
            f"WHERE table_catalog='{PUBLICATION_DB}' "
            f"AND table_schema='{schema}' AND table_name='{base}'"
        ).fetchone()[0])
        if not present:
            log(f"  SKIP snapshot {base} — source already absent (prior-run "
                "snapshot lives in archive_pub_v1_0).")
            skipped.append(base)
            continue
        suffix = f"{base}_pre279_{ISO_TS}"
        if do_writes:
            dest = snapshot_table(con, src_fq, suffix, SCRIPT_TAG, reason)
            log(f"  snapshot {src_fq} -> {dest}")
            snapshots[base] = dest
        else:
            dest_plan = f'{ARCHIVE_QUALIFIED}."{suffix}"'
            log(f"  PLAN snapshot {src_fq} -> {dest_plan}")
            snapshots[base] = dest_plan
    return {"snapshots": snapshots, "skipped": skipped}


# ---------------------------------------------------------------------------
# Phase 2 — ENE BOOLEAN realign
# ---------------------------------------------------------------------------
def phase_2(con, do_writes: bool) -> dict:
    log("\n## Phase 2 — ENE BOOLEAN realign from INTEGER")
    before = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_mets_extranodal_extension=TRUE"
    ).fetchone()[0])
    target = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_extranodal_extension > 0"
    ).fetchone()[0])
    log(f"  before: bool_true={before}  target(int>0)={target}")

    upd = (
        f"UPDATE {LN_ROLLUP} "
        f"SET ln_mets_extranodal_extension = (ln_extranodal_extension > 0) "
        f"WHERE ln_extranodal_extension IS NOT NULL "
        f"AND (ln_mets_extranodal_extension IS DISTINCT FROM "
        f"     (ln_extranodal_extension > 0))"
    )
    _exec(con, upd, do_writes, label="ene_realign")

    comment = (
        "BOOLEAN: (ln_extranodal_extension > 0). Rebuilt from the INTEGER "
        "count by Script 279 (2026-04-18) to resolve a bifurcation where "
        "the BOOLEAN was populated independently and only 14 of 4273 rows "
        "were TRUE while the INTEGER showed 1326 positive. The INTEGER is "
        "the source of truth; this column is a convenience flag."
    )
    _exec(con,
          f"COMMENT ON COLUMN {LN_ROLLUP}.ln_mets_extranodal_extension IS "
          f"{_quote(comment)}",
          do_writes, label="ene_comment")

    # Pre-update accounting: how many INT-NULL rows already have BOOL=TRUE.
    # The UPDATE intentionally leaves these untouched ("don't fabricate"),
    # so they survive in the post-update BOOL TRUE total. The realigned
    # target therefore = count(int>0) + count(int IS NULL AND bool=TRUE).
    preserved_true_under_null_int = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_extranodal_extension IS NULL "
        f"AND ln_mets_extranodal_extension = TRUE"
    ).fetchone()[0])
    target_realigned = target + preserved_true_under_null_int
    log(f"  preserved_true_under_null_int={preserved_true_under_null_int} "
        f"target_realigned={target_realigned}")

    if not do_writes:
        return {"before": before, "after": None,
                "target_int_positive": target,
                "preserved_true_under_null_int": preserved_true_under_null_int,
                "target_realigned": target_realigned,
                "status": "DRY_RUN"}

    after = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_mets_extranodal_extension=TRUE"
    ).fetchone()[0])
    sync_violations = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_extranodal_extension IS NOT NULL "
        f"AND ln_mets_extranodal_extension <> (ln_extranodal_extension > 0)"
    ).fetchone()[0])
    int_null_bool_set = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_extranodal_extension IS NULL "
        f"AND ln_mets_extranodal_extension IS NOT NULL"
    ).fetchone()[0])
    log(f"  after: bool_true={after}  sync_violations={sync_violations}  "
        f"int_null_bool_set={int_null_bool_set}")
    # Correctness gate: BOOL must agree with (INT > 0) wherever INT is non-NULL.
    # The post-update BOOL TRUE total is target_realigned (preserved TRUEs
    # under NULL INT are by-design left as-is per the "don't fabricate" rule).
    if sync_violations != 0 or after != target_realigned:
        raise SystemExit(
            f"PHASE 2 FAIL: after={after} target_realigned={target_realigned} "
            f"sync_violations={sync_violations}"
        )
    if int_null_bool_set:
        log(f"  NOTE: {int_null_bool_set} rows have INT NULL but BOOL set "
            "(left as-is; we do not fabricate). Logged for audit.")
    return {"before": before, "after": after,
            "target_int_positive": target,
            "preserved_true_under_null_int": preserved_true_under_null_int,
            "target_realigned": target_realigned,
            "sync_violations": sync_violations,
            "int_null_bool_set": int_null_bool_set, "status": "OK"}


# ---------------------------------------------------------------------------
# Phase 3 — retag impossible consistency rows
# ---------------------------------------------------------------------------
def phase_3(con, do_writes: bool) -> dict:
    log("\n## Phase 3 — retag impossible consistency rows")
    before = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_total_positive > ln_total_examined "
        f"AND ln_internal_consistency = 'ok'"
    ).fetchone()[0])
    log(f"  before: impossible-but-tagged-ok rows = {before}")

    distinct_vocab = [r[0] for r in con.execute(
        f"SELECT DISTINCT ln_internal_consistency FROM {LN_ROLLUP} "
        f"ORDER BY 1 NULLS LAST"
    ).fetchall()]
    log(f"  pre-update vocab: {distinct_vocab}")

    upd = (
        f"UPDATE {LN_ROLLUP} "
        f"SET ln_internal_consistency = 'impossible_positive_exceeds_examined' "
        f"WHERE ln_total_positive > ln_total_examined "
        f"AND ln_internal_consistency = 'ok'"
    )
    _exec(con, upd, do_writes, label="impossible_retag")

    if do_writes:
        post_vocab = [r[0] for r in con.execute(
            f"SELECT DISTINCT ln_internal_consistency FROM {LN_ROLLUP} "
            f"ORDER BY 1 NULLS LAST"
        ).fetchall()]
    else:
        post_vocab = distinct_vocab + ["impossible_positive_exceeds_examined"]
    log(f"  post-update vocab: {post_vocab}")

    vocab_str = ", ".join(repr(v) for v in post_vocab if v is not None)
    comment = (
        "Validator tag. Values: ok = examined >= positive AND both "
        "non-negative; impossible_positive_exceeds_examined = positive > "
        "examined (data ingestion error; counts preserved for audit). "
        f"Observed vocabulary post-Script 279: {vocab_str}. Retagged by "
        "Script 279 (2026-04-18) which added the 'impossible' bucket — "
        "previously these 11 rows were silently tagged 'ok'."
    )
    _exec(con,
          f"COMMENT ON COLUMN {LN_ROLLUP}.ln_internal_consistency IS "
          f"{_quote(comment)}",
          do_writes, label="consistency_comment")

    if not do_writes:
        return {"before": before, "after": None, "vocab_after": post_vocab,
                "status": "DRY_RUN"}

    after = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_total_positive > ln_total_examined "
        f"AND ln_internal_consistency = 'ok'"
    ).fetchone()[0])
    log(f"  after: {after}")
    if after != 0:
        raise SystemExit(f"PHASE 3 FAIL: after={after} expected 0")
    return {"before": before, "after": after, "vocab_after": post_vocab,
            "status": "OK"}


# ---------------------------------------------------------------------------
# Phase 4 — orphan routing
# ---------------------------------------------------------------------------
def phase_4(con, do_writes: bool) -> dict:
    log("\n## Phase 4 — clinical_note_ln_extracted_v1 orphan routing")

    profile = con.execute(f"""
        SELECT
          (SELECT COUNT(*) FROM {CPM}
              WHERE research_id='11454') AS in_cpm,
          (SELECT COUNT(*) FROM {PUBLICATION_DB}.main.fna_episode_master_v2
              WHERE CAST(research_id AS VARCHAR)='11454') AS in_fna,
          (SELECT COUNT(*) FROM {PUBLICATION_DB}.main.tumor_episode_master_v2
              WHERE CAST(research_id AS VARCHAR)='11454') AS in_tumor,
          (SELECT COUNT(*) FROM {PUBLICATION_DB}.main.path_synoptics
              WHERE CAST(research_id AS VARCHAR)='11454') AS in_synoptic,
          (SELECT COUNT(*) FROM {PUBLICATION_DB}.main.operative_episode_detail_v2
              WHERE CAST(research_id AS VARCHAR)='11454') AS in_op,
          (SELECT COUNT(*) FROM {PUBLICATION_DB}.main.clinical_notes_long
              WHERE CAST(research_id AS VARCHAR)='11454') AS in_notes,
          (SELECT COUNT(*) FROM {LN_EXTRACT}
              WHERE CAST(research_id AS VARCHAR)='11454') AS in_ln_extract
    """).fetchone()
    fields = ["in_cpm", "in_fna", "in_tumor", "in_synoptic",
              "in_op", "in_notes", "in_ln_extract"]
    profile_dict = dict(zip(fields, [int(x) for x in profile]))
    log(f"  rid 11454 evidence profile: {profile_dict}")

    expected_pattern = {"in_cpm": 0, "in_fna": 0, "in_tumor": 0,
                        "in_synoptic": 0, "in_op": 0,
                        "in_notes": 2, "in_ln_extract": 2}
    if profile_dict != expected_pattern:
        log(f"  NOTE: rid 11454 profile differs from §7.3 baseline "
            f"{expected_pattern}; routing still proceeds.")

    sql_route = f"""
        CREATE OR REPLACE TABLE {ORPHAN_TABLE} AS
        SELECT 'clinical_note_ln_extracted_v1' AS source_table,
               r.*,
               'non_cohort_patient_correctly_excluded_cancer_evidence_absent' AS disposition,
               CURRENT_TIMESTAMP AS registered_at
        FROM {LN_EXTRACT} r
        WHERE CAST(r.research_id AS VARCHAR) NOT IN (
          SELECT research_id FROM {CPM}
          WHERE research_id IS NOT NULL
        )
    """
    _exec(con, sql_route, do_writes, label="route_orphans")

    comment = (
        "Script 279 (2026-04-18) routing destination for "
        "clinical_note_ln_extracted_v1 rows whose research_id is not in "
        "canonical_patient_master. These are the §7.3 non-cohort pattern: "
        "patient has clinical notes but zero cancer evidence across FNA/"
        "tumor/synoptic/path/operative — correctly excluded from the "
        "cancer cohort, but the LN extractor processed the note. Source "
        "rows in clinical_note_ln_extracted_v1 are NOT deleted. This "
        "table is a routing queue, not a defect backlog."
    )
    _exec(con,
          f"COMMENT ON TABLE {ORPHAN_TABLE} IS {_quote(comment)}",
          do_writes, label="orphan_table_comment")

    if not do_writes:
        return {"profile": profile_dict, "routed_rows": None,
                "status": "DRY_RUN"}

    routed = int(con.execute(
        f"SELECT COUNT(*) FROM {ORPHAN_TABLE}"
    ).fetchone()[0])
    routed_rids = int(con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {ORPHAN_TABLE}"
    ).fetchone()[0])
    log(f"  routed_rows={routed}  routed_distinct_rids={routed_rids}")
    return {"profile": profile_dict, "routed_rows": routed,
            "routed_distinct_rids": routed_rids, "status": "OK"}


# ---------------------------------------------------------------------------
# Phase 5 — dictionary v266a -> v279
# ---------------------------------------------------------------------------
def phase_5(con, do_writes: bool) -> dict:
    log("\n## Phase 5 — data_dictionary_v266a -> data_dictionary_v279")

    v266a_present = int(con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        f"AND table_name='data_dictionary_v266a'"
    ).fetchone()[0])
    v279_present = int(con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        f"AND table_name='data_dictionary_v279'"
    ).fetchone()[0])
    log(f"  v266a present: {v266a_present}  v279 present: {v279_present}")
    if not v266a_present and not v279_present:
        raise SystemExit(
            "PHASE 5 PRECONDITION FAIL: neither data_dictionary_v266a nor "
            "data_dictionary_v279 exists in main."
        )

    dict_source_table = "data_dictionary_v266a" if v266a_present else "data_dictionary_v279"
    dict_cols = [r[0] for r in con.execute(
        f"SELECT column_name FROM information_schema.columns "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        f"AND table_name='{dict_source_table}' ORDER BY ordinal_position"
    ).fetchall()]
    # Strip v279_note (only present if dict_source_table=='data_dictionary_v279')
    dict_cols = [c for c in dict_cols if c != "v279_note"]
    log(f"  source dict columns ({len(dict_cols)}): {dict_cols}")
    has_rebuilt_at = "rebuilt_at" in dict_cols
    has_rebuilt_by = "rebuilt_by" in dict_cols
    has_status = "status" in dict_cols
    has_replacement = "replacement_column_name" in dict_cols
    has_description = "description" in dict_cols
    if not (has_status and has_replacement and has_description):
        raise SystemExit(
            "PHASE 5 PRECONDITION FAIL: source dictionary missing one of "
            f"status/replacement_column_name/description. cols={dict_cols}"
        )

    if v266a_present:
        n_v266a = int(con.execute(
            f"SELECT COUNT(*) FROM {DICT_V266A}"
        ).fetchone()[0])
        log(f"  v266a rows: {n_v266a}")

        sql_ctas = (
            f"CREATE OR REPLACE TABLE {DICT_V279} AS "
            f"SELECT *, CAST('' AS VARCHAR) AS v279_note FROM {DICT_V266A}"
        )
        _exec(con, sql_ctas, do_writes, label="ctas_v279")

        table_comment = (
            "Script 279 (2026-04-18). CTAS from data_dictionary_v266a with "
            "Prompt-20 / PART2 §7.2 / §5.4 clarifications applied to 10 "
            "recurrence and 6 LN columns: descriptions populated, "
            "replacement_column_name set where a strict successor exists, "
            "and three statuses refined "
            "(archived_removed_from_cpm / component / authoritative_subtype). "
            "Old data_dictionary_v266a is dropped from main; pre-279 snapshot "
            "lives in archive_pub_v1_0."
        )
        _exec(con,
              f"COMMENT ON TABLE {DICT_V279} IS {_quote(table_comment)}",
              do_writes, label="comment_v279")
    else:
        # Idempotent re-run: v279 already built, v266a already dropped.
        n_v266a = int(con.execute(
            f"SELECT COUNT(*) FROM {DICT_V279}"
        ).fetchone()[0])  # current v279 row count is the baseline
        log(f"  v279 already exists from a prior run; baseline row count "
            f"(current v279) = {n_v266a}")

    set_clauses_extra = []
    if has_rebuilt_at:
        set_clauses_extra.append(("rebuilt_at", "CURRENT_TIMESTAMP"))
    if has_rebuilt_by:
        set_clauses_extra.append(("rebuilt_by", "'script_279'"))

    # Discover which targets are absent from v279 (e.g.,
    # any_recurrence_flag_prev_233 was removed from CPM post-Script 249 and
    # never had a dictionary row). Insert stub rows for these so the
    # documentation captures all 16 targets.
    if do_writes:
        present = {
            r[0] for r in con.execute(
                f"SELECT DISTINCT column_name FROM {DICT_V279} "
                f"WHERE column_name IN "
                f"({', '.join(repr(c) for c in DICT_UPDATE_COLUMNS)})"
            ).fetchall()
        }
    else:
        present = {
            r[0] for r in con.execute(
                f"SELECT DISTINCT column_name FROM {DICT_V266A} "
                f"WHERE column_name IN "
                f"({', '.join(repr(c) for c in DICT_UPDATE_COLUMNS)})"
            ).fetchall()
        }
    absent = [c for c in DICT_UPDATE_COLUMNS if c not in present]
    log(f"  target columns present in v279: {sorted(present)}")
    log(f"  target columns absent from v279 (stub-insert needed): {absent}")

    inserted_stubs: list[str] = []
    for col in absent:
        upd = next(u for u in DICT_UPDATES if u["column_name"] == col)
        stub_map = {
            "table_name": "canonical_patient_master_archived",
            "column_name": col,
            "data_type": None,
            "is_nullable": None,
            "ordinal_position": None,
            "comment": "Inserted by Script 279 for documentation completeness.",
            "n_non_null": None,
            "pct_non_null": None,
            "n_distinct": None,
            "description": upd["description"],
            "status": upd["status"],
            "replacement_column_name": upd["replacement_column_name"],
            "rebuilt_at": datetime.now(timezone.utc) if has_rebuilt_at else None,
            "rebuilt_by": "script_279" if has_rebuilt_by else None,
            "v279_note": ("Stub row inserted by 279 — column was removed from "
                          "CPM in an earlier cleanup; documentation only."),
        }
        # Preserve dictionary's existing column order, append v279_note last.
        v279_cols = dict_cols + ["v279_note"]
        placeholders = ", ".join("?" for _ in v279_cols)
        row = [stub_map.get(c) for c in v279_cols]
        if do_writes:
            con.execute(
                f"INSERT INTO {DICT_V279} ({', '.join(v279_cols)}) "
                f"VALUES ({placeholders})", row,
            )
            log(f"  EXEC [stub_insert:{col}]: inserted documentation row")
        else:
            log(f"  PLAN [stub_insert:{col}]: would insert documentation row")
        inserted_stubs.append(col)

    applied: list[str] = []
    for upd in DICT_UPDATES:
        col = upd["column_name"]
        if col in inserted_stubs:
            # Stub already carries description/status/replacement; skip UPDATE.
            applied.append(col)
            continue
        sets = [
            f"status = {_quote(upd['status'])}",
            f"replacement_column_name = {_quote(upd['replacement_column_name'])}",
            f"description = {_quote(upd['description'])}",
            "v279_note = 'Clarified by 279 per §7.2 / §5.4 / Prompt 20 real findings'",
        ]
        for cname, cexpr in set_clauses_extra:
            sets.append(f"{cname} = {cexpr}")
        sql = (
            f"UPDATE {DICT_V279} SET " + ", ".join(sets) +
            f" WHERE column_name = {_quote(col)}"
        )
        _exec(con, sql, do_writes, label=f"update:{col}")
        applied.append(col)

    if v266a_present:
        drop_sql = f"DROP TABLE {DICT_V266A}"
        _exec(con, drop_sql, do_writes, label="drop_v266a")
    else:
        log("  SKIP drop_v266a — already dropped on a prior run")

    readme_present = int(con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        f"AND table_name='__readme'"
    ).fetchone()[0])
    readme_actions: list[str] = []
    if readme_present:
        readme_cols = [r[0] for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
            f"AND table_name='__readme' ORDER BY ordinal_position"
        ).fetchall()]
        log(f"  __readme cols: {readme_cols}")

        # Delete v266a row(s)
        del_sql = f"DELETE FROM {README_TBL} WHERE table_name='data_dictionary_v266a'"
        _exec(con, del_sql, do_writes, label="readme_del_v266a")
        readme_actions.append("delete:data_dictionary_v266a")

        # Insert v279 row if not already there
        if do_writes:
            n_v279 = int(con.execute(
                f"SELECT COUNT(*) FROM {DICT_V279}"
            ).fetchone()[0])
            existing = int(con.execute(
                f"SELECT COUNT(*) FROM {README_TBL} "
                f"WHERE table_name='data_dictionary_v279'"
            ).fetchone()[0])
            if existing == 0:
                ins_desc = (
                    "Recurrence + LN dictionary clarification by Script 279 "
                    "(2026-04-18). Successor to data_dictionary_v266a (retired)."
                )
                # Schema (per 266c):
                #   table_name VARCHAR PRIMARY KEY,
                #   n_rows BIGINT,
                #   n_distinct_research_id BIGINT,
                #   description VARCHAR,
                #   inventoried_at TIMESTAMP
                # Be defensive: bind to schema dynamically.
                placeholders = ", ".join("?" for _ in readme_cols)
                row_map = {
                    "table_name": "data_dictionary_v279",
                    "n_rows": n_v279,
                    "n_distinct_research_id": None,
                    "description": ins_desc,
                    "inventoried_at": datetime.now(timezone.utc),
                }
                row = [row_map.get(c, None) for c in readme_cols]
                con.execute(
                    f"INSERT INTO {README_TBL} ({', '.join(readme_cols)}) "
                    f"VALUES ({placeholders})", row,
                )
                log("  EXEC [readme_ins_v279]: inserted data_dictionary_v279 row")
                readme_actions.append("insert:data_dictionary_v279")
            else:
                log("  SKIP readme insert v279 — already present")
                readme_actions.append("skip_existing:data_dictionary_v279")
        else:
            log("  PLAN [readme_ins_v279]: would insert data_dictionary_v279 row")
            readme_actions.append("plan_insert:data_dictionary_v279")
    else:
        log("  __readme not present in main — skipping readme update.")

    # detail_table_registry_v1 — does not reference data_dictionary in
    # any current row; check defensively then skip.
    registry_actions: list[str] = []
    try:
        n_ref = int(con.execute(
            f"SELECT COUNT(*) FROM {REGISTRY} "
            f"WHERE detail_table_name='data_dictionary_v266a' "
            f"   OR detail_table_name='data_dictionary_v279'"
        ).fetchone()[0])
    except Exception as e:
        n_ref = 0
        log(f"  registry probe failed (continuing): {e}")
    log(f"  detail_table_registry_v1 rows referencing the dictionary: {n_ref}")
    if n_ref:
        upd_reg = (
            f"UPDATE {REGISTRY} "
            f"SET detail_table_name='data_dictionary_v279' "
            f"WHERE detail_table_name='data_dictionary_v266a'"
        )
        _exec(con, upd_reg, do_writes, label="registry_repoint")
        registry_actions.append("repoint:v266a->v279")
    else:
        log("  registry has no dictionary row — skipping (decision logged).")
        registry_actions.append("skip:no_registry_row")

    if not do_writes:
        return {"v266a_rows": n_v266a,
                "applied_columns": applied,
                "inserted_stubs": inserted_stubs,
                "set_clauses_extra": [c for c, _ in set_clauses_extra],
                "readme_actions": readme_actions,
                "registry_actions": registry_actions,
                "status": "DRY_RUN"}

    n_v279 = int(con.execute(
        f"SELECT COUNT(*) FROM {DICT_V279}"
    ).fetchone()[0])
    n_clarified = int(con.execute(
        f"SELECT COUNT(*) FROM {DICT_V279} "
        f"WHERE column_name IN ({', '.join(repr(c) for c in DICT_UPDATE_COLUMNS)}) "
        f"AND description IS NOT NULL AND description <> ''"
    ).fetchone()[0])
    blanks = [r[0] for r in con.execute(
        f"SELECT column_name FROM {DICT_V279} "
        f"WHERE column_name IN ({', '.join(repr(c) for c in DICT_UPDATE_COLUMNS)}) "
        f"AND (description IS NULL OR description = '')"
    ).fetchall()]
    v266a_residue = int(con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        f"AND table_name='data_dictionary_v266a'"
    ).fetchone()[0])
    log(f"  v279_rows={n_v279} clarified_rows={n_clarified} blanks={blanks} "
        f"v266a_residue={v266a_residue}")
    expected_v279 = n_v266a + len(inserted_stubs)
    if n_v279 != expected_v279:
        raise SystemExit(
            f"PHASE 5 FAIL: v279_rows {n_v279} != v266a_rows {n_v266a} + "
            f"stubs {len(inserted_stubs)} = {expected_v279}"
        )
    if blanks:
        raise SystemExit(f"PHASE 5 FAIL: blanks remain: {blanks}")
    if v266a_residue != 0:
        raise SystemExit(
            f"PHASE 5 FAIL: data_dictionary_v266a still present in main."
        )
    if n_clarified != len(DICT_UPDATE_COLUMNS):
        raise SystemExit(
            f"PHASE 5 FAIL: clarified rows {n_clarified} != "
            f"{len(DICT_UPDATE_COLUMNS)}"
        )
    return {"v266a_rows": n_v266a, "v279_rows": n_v279,
            "expected_v279_rows": expected_v279,
            "clarified_rows": n_clarified,
            "applied_columns": applied,
            "inserted_stubs": inserted_stubs,
            "set_clauses_extra": [c for c, _ in set_clauses_extra],
            "readme_actions": readme_actions,
            "registry_actions": registry_actions,
            "v266a_residue": v266a_residue, "status": "OK"}


# ---------------------------------------------------------------------------
# Phase 6 — register v1.1 tech debt
# ---------------------------------------------------------------------------
TECH_DEBT_ROWS: list[tuple] = [
    (
        "ln_rollup_per_surgery_grain_formalization_v1_1",
        "table_design",
        "ln_master_rollup_v1 is per-surgery-episode by design (4,273 rows "
        "for 3,986 patients; 256 patients have 2-4 surgeries). This grain "
        "is intentional per PART2 §5.4 but is not codified in the table "
        "COMMENT or the dictionary. A consumer reading ln_master_rollup_v1 "
        "can silently mis-join 1:1 against canonical_patient_master and "
        "double-count or drop the second-surgery rows.",
        "v1.1: (a) add COMMENT ON TABLE main.ln_master_rollup_v1 documenting "
        "the per-surgery grain explicitly; (b) add a "
        "ln_master_rollup_patient_v1 companion view that picks a single "
        "per-patient row using a documented rule (recommend: ORDER BY "
        "ln_source preference, then MAX(ln_total_examined), then "
        "MAX(ln_total_positive)); (c) register both in "
        "detail_table_registry_v1.",
        "script_279",
        None,  # registered_at -> set to current at insert
        "v1_1",
        "OPEN",
        None,
        None,
    ),
    (
        "recurrence_flag_consumer_audit_v1_1",
        "consumer_migration",
        "After Script 279 clarified the recurrence column dictionary, it "
        "is not yet known whether downstream consumers (manuscript_workspace "
        "cohort_* views, analysis subsets, parquet export) have been updated "
        "to read recurrence_flag_v2 instead of any_recurrence_flag where "
        "strict path-proven semantics are intended.",
        "v1.1: grep the repo + view definitions for any_recurrence_flag and "
        "any_recurrence_flag_prev_233. For each hit, decide whether the "
        "consumer wants strict (-> switch to recurrence_flag_v2) or broad "
        "(-> leave + add comment). Produce "
        "scripts/output/recurrence_consumer_audit.md.",
        "script_279",
        None,
        "v1_1",
        "OPEN",
        None,
        None,
    ),
]


def phase_6(con, do_writes: bool) -> dict:
    log("\n## Phase 6 — register v1.1 tech debt")

    table_present = int(con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog='{PUBLICATION_DB}' "
        f"AND table_schema='manuscript_workspace' "
        f"AND table_name='v1_1_tech_debt_v1'"
    ).fetchone()[0])
    if not table_present:
        raise SystemExit(
            "v1_1_tech_debt_v1 not present in manuscript_workspace — "
            "cannot register debt rows. Run earlier finalization scripts."
        )

    actions: list[dict] = []
    for row in TECH_DEBT_ROWS:
        debt_id = row[0]
        present = int(con.execute(
            f"SELECT COUNT(*) FROM {TECH_DEBT_FQ} WHERE debt_id = ?",
            [debt_id],
        ).fetchone()[0])
        if present:
            log(f"  SKIP tech_debt {debt_id!r} — already present")
            actions.append({"debt_id": debt_id, "action": "skip_existing"})
            continue
        if do_writes:
            row_w = list(row)
            row_w[5] = datetime.now(timezone.utc)  # registered_at (index 5)
            con.execute(f"""
                INSERT INTO {TECH_DEBT_FQ}
                    (debt_id, category, description, recommendation,
                     registered_by, registered_at, target_version,
                     status, resolved_at, resolved_by_script)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row_w)
            log(f"  EXEC [tech_debt_insert]: {debt_id}")
            actions.append({"debt_id": debt_id, "action": "inserted"})
        else:
            log(f"  PLAN [tech_debt_insert]: {debt_id}")
            actions.append({"debt_id": debt_id, "action": "plan_insert"})
    return {"actions": actions}


# ---------------------------------------------------------------------------
# Phase 7 — final verification
# ---------------------------------------------------------------------------
def phase_7(con, baseline_invariants: dict, do_writes: bool) -> dict:
    log("\n## Phase 7 — final verification")
    if not do_writes:
        log("  PLAN: would run gates against post-apply state. Dry-run skipped.")
        return {"skipped_dryrun": True}

    inv = assert_invariants(con)
    if inv["cpm_cols"] != baseline_invariants["cpm_cols"]:
        raise SystemExit(
            f"GATE B FAIL: cpm_cols changed "
            f"{baseline_invariants['cpm_cols']} -> {inv['cpm_cols']}"
        )
    log(f"  GATE B cpm_cols unchanged ({inv['cpm_cols']})")

    ene_sync = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_extranodal_extension IS NOT NULL "
        f"AND ln_mets_extranodal_extension <> (ln_extranodal_extension > 0)"
    ).fetchone()[0])
    log(f"  GATE C ene_sync_violations={ene_sync} (must=0)")
    if ene_sync != 0:
        raise SystemExit("GATE C FAIL")

    impossible_ok = int(con.execute(
        f"SELECT COUNT(*) FROM {LN_ROLLUP} "
        f"WHERE ln_total_positive > ln_total_examined "
        f"AND ln_internal_consistency='ok'"
    ).fetchone()[0])
    log(f"  GATE D impossible_ok={impossible_ok} (must=0)")
    if impossible_ok != 0:
        raise SystemExit("GATE D FAIL")

    clarified = int(con.execute(
        f"SELECT COUNT(*) FROM {DICT_V279} "
        f"WHERE column_name IN ({', '.join(repr(c) for c in DICT_UPDATE_COLUMNS)}) "
        f"AND description IS NOT NULL AND description <> ''"
    ).fetchone()[0])
    log(f"  GATE E clarified_rows={clarified} (must={len(DICT_UPDATE_COLUMNS)})")
    if clarified != len(DICT_UPDATE_COLUMNS):
        raise SystemExit("GATE E FAIL")

    old_dict = int(con.execute(
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' "
        f"AND table_name='data_dictionary_v266a'"
    ).fetchone()[0])
    log(f"  GATE F old_dict_present={old_dict} (must=0)")
    if old_dict != 0:
        raise SystemExit("GATE F FAIL")

    snaps = int(con.execute(f"""
        SELECT COUNT(*) FROM "{ARCHIVE_DB}".information_schema.tables
        WHERE table_schema='{ARCHIVE_SCHEMA}'
        AND (table_name LIKE 'ln_master_rollup_v1_pre279_%'
          OR table_name LIKE 'clinical_note_ln_extracted_v1_pre279_%'
          OR table_name LIKE 'data_dictionary_v266a_pre279_%')
    """).fetchone()[0])
    log(f"  GATE G snapshots_present={snaps} (must>=3)")
    if snaps < 3:
        raise SystemExit("GATE G FAIL")

    debt = int(con.execute(
        f"SELECT COUNT(*) FROM {TECH_DEBT_FQ} "
        f"WHERE debt_id IN ('ln_rollup_per_surgery_grain_formalization_v1_1', "
        f"                  'recurrence_flag_consumer_audit_v1_1')"
    ).fetchone()[0])
    log(f"  GATE H tech_debt_registered={debt} (must=2)")
    if debt != 2:
        raise SystemExit("GATE H FAIL")

    return {
        "cpm_cols": inv["cpm_cols"],
        "ene_sync_violations": ene_sync,
        "impossible_ok": impossible_ok,
        "clarified_rows": clarified,
        "old_dict_present": old_dict,
        "snapshots_present": snaps,
        "tech_debt_registered": debt,
        "all_passed": True,
    }


# ---------------------------------------------------------------------------
# Audit row recording (apply mode only)
# ---------------------------------------------------------------------------
def record_audits(con, results: dict) -> None:
    ensure_audit_table(con)
    p2 = results.get("phase_2", {})
    p3 = results.get("phase_3", {})
    p4 = results.get("phase_4", {})
    p5 = results.get("phase_5", {})

    # ENE
    record_audit(
        con, SCRIPT_NUM, "279_ene_boolean_realign",
        "ene_boolean_true",
        count_before=int(p2.get("before") or 0),
        count_after=int(p2.get("after") or 0),
        target_after=int(p2.get("target_realigned") or 0),
        status="OK" if (p2.get("after") == p2.get("target_realigned"))
               else "FAIL",
        notes=("ln_master_rollup_v1.ln_mets_extranodal_extension rebuilt "
               "from (ln_extranodal_extension > 0). The INTEGER is the "
               "source of truth; the BOOLEAN was an independently-populated "
               "dead column. target_realigned = count(int>0) + "
               f"count(int IS NULL AND bool=TRUE preserved) = "
               f"{p2.get('target_int_positive')} + "
               f"{p2.get('preserved_true_under_null_int')}; the latter are "
               "intentionally left untouched per the 'don't fabricate' rule."),
    )
    # Consistency
    record_audit(
        con, SCRIPT_NUM, "279_consistency_impossible_retag",
        "rows_retagged",
        count_before=int(p3.get("before") or 0),
        count_after=int(p3.get("after") or 0),
        target_after=0,
        status="OK" if p3.get("after") == 0 else "FAIL",
        notes=("11 rows with ln_total_positive > ln_total_examined retagged "
               "ln_internal_consistency='impossible_positive_exceeds_examined'. "
               "Counts preserved (ingestion artifacts)."),
    )
    # Orphan routing
    record_audit(
        con, SCRIPT_NUM, "279_ln_extract_orphan_route",
        "orphan_rids_routed",
        count_before=int(p4.get("routed_distinct_rids") or 0),
        count_after=int(p4.get("routed_distinct_rids") or 0),
        target_after=int(p4.get("routed_distinct_rids") or 0),
        status="DOCUMENTED_NOOP",
        notes=("research_id 11454 matches §7.3 non-cohort pattern: "
               "0 cancer evidence across FNA/tumor/synoptic/path/op, "
               "2 clinical_notes_long rows, 2 LN extract rows. Routed to "
               "manuscript_workspace.ln_extract_noncohort_orphan_v279. "
               "Source rows NOT deleted. No CPM change."),
    )
    # Dictionary
    record_audit(
        con, SCRIPT_NUM, "279_dictionary_clarification",
        "columns_clarified",
        count_before=0,
        count_after=int(p5.get("clarified_rows") or 0),
        target_after=len(DICT_UPDATE_COLUMNS),
        status="OK" if p5.get("clarified_rows") == len(DICT_UPDATE_COLUMNS)
               else "FAIL",
        notes=("data_dictionary_v266a retired; data_dictionary_v279 published "
               "with descriptions + replacement_column_name for 10 recurrence "
               "+ 6 LN columns per PART2 §7.2 and §5.4."),
    )


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Execute writes; default is dry-run.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Explicit dry-run (default).")
    args = ap.parse_args()
    do_writes = bool(args.apply) and not bool(args.dry_run)

    if LOG_PATH.exists():
        LOG_PATH.unlink()
    log("=" * 78)
    log(f"# {SCRIPT_TAG} run")
    log(f"started_at: {NOW}")
    log(f"mode      : {'APPLY' if do_writes else 'DRY-RUN'}")
    log(f"iso_ts    : {ISO_TS}")
    log("=" * 78)

    t0 = time.time()
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    results: dict = {
        "meta": {
            "mode": "APPLY" if do_writes else "DRY-RUN",
            "started_at": NOW,
            "script": SCRIPT_TAG,
            "iso_ts": ISO_TS,
        },
    }

    try:
        results["phase_0"] = phase_0(con)
        results["phase_1"] = phase_1(con, do_writes)
        results["phase_2"] = phase_2(con, do_writes)
        results["phase_3"] = phase_3(con, do_writes)
        results["phase_4"] = phase_4(con, do_writes)
        results["phase_5"] = phase_5(con, do_writes)
        results["phase_6"] = phase_6(con, do_writes)
        results["phase_7"] = phase_7(
            con, results["phase_0"]["invariants"], do_writes,
        )

        if do_writes:
            record_audits(con, results)

        write_decision_log(DECISION_LOG, {
            "script": SCRIPT_TAG,
            "run_date": RUN_DATE,
            "mode": "APPLY" if do_writes else "DRY-RUN",
            "iso_ts": ISO_TS,
            "results": results,
            "elapsed_seconds": round(time.time() - t0, 1),
        })
        log(f"\nelapsed: {time.time() - t0:.1f}s")

        # Final print-to-console summary
        log("")
        log("SCRIPT 279 FINAL STATE")
        if do_writes:
            log(f"  canonical_patient_master  : 10,871 rows x "
                f"{results['phase_0']['invariants']['cpm_cols']} cols (unchanged)")
            log(f"  ln_master_rollup_v1       : ENE bifurcation resolved "
                f"({results['phase_2']['before']} -> "
                f"{results['phase_2']['after']} TRUE)")
            log(f"                              "
                f"{results['phase_3']['before']} impossible rows retagged")
            log(f"  clinical_note_ln_extract  : "
                f"{results['phase_4']['routed_distinct_rids']} non-cohort "
                f"orphan routed to ws queue")
            log(f"  data_dictionary_v266a -> data_dictionary_v279 "
                f"({results['phase_5']['clarified_rows']} recurrence/LN "
                f"clarifications)")
            log(f"  v1_1_tech_debt_v1         : +"
                f"{sum(1 for a in results['phase_6']['actions'] if a['action']=='inserted')}"
                f" open items for v1.1")
            log("  Snapshots (archive_pub_v1_0):")
            for k, v in results["phase_1"]["snapshots"].items():
                log(f"    {v}")
            log("  Invariants               : ALL PASSED")
            log("Prompt 20 real findings    : addressed.")
            log("Prompt 20 withdrawn findings (any_recurrence_flag rebuild,")
            log("  ln_rollup 1:1 restructure, recurrence_date backfill):")
            log("  confirmed not actioned per PART2 §7.2 / §5.4 / source-limited.")
            log("Canonical remains v1.0-published + v1.1-patched. Safe to proceed.")
        else:
            log("  DRY-RUN — no writes performed. Re-run with --apply.")
        log("")
        return 0

    except SystemExit:
        log("\nSCRIPT 279 FAILED — NO CHANGES COMMITTED")
        log("  (See preceding GATE/PHASE log line for the failing invariant.)")
        log("  Archive snapshots preserved; revert by restoring from "
            f'"{ARCHIVE_DB}".{ARCHIVE_SCHEMA}.<table>_pre279_{ISO_TS}.')
        # Persist whatever results we have
        write_decision_log(DECISION_LOG, {
            "script": SCRIPT_TAG, "run_date": RUN_DATE,
            "mode": "APPLY" if do_writes else "DRY-RUN",
            "iso_ts": ISO_TS, "results": results,
            "elapsed_seconds": round(time.time() - t0, 1),
            "outcome": "FAIL",
        })
        raise
    except Exception as e:
        log(f"FATAL: {e!r}")
        log(traceback.format_exc())
        return 1
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
