#!/usr/bin/env python3
"""Script 364 — CPM feeder repoint to canonical complications canonicals.

Commit 2 of the 3-commit cascade for Script 364:
    1. scripts/364_complications_consolidation.py --commit
       (build canonicals + registry + QA + CPM audit report)
    2. scripts/364_cpm_feeder_repoint.py --commit              ← THIS
       (repoint CPM columns to read from new canonicals; write marker)
    3. scripts/364_complications_consolidation.py --commit --phase 7
       (drop the 5 deprecated source tables, gated on the marker file)

What this script does
=====================
1. Snapshot CPM (full row copy) to archive_pub_v1_0 as a parity guard.
2. Validate all rollup columns referenced by the repoint plan exist.
3. For each of the existing CPM BOOLEAN columns in the REPOINT list,
   UPDATE its value to read from canonical_complications_patient_rollup_v1
   on (research_id). Patients without a rollup row default to FALSE.
4. Log per-column TRUE/FALSE/NULL deltas (pre vs post).
5. Document the explicit SKIP set (CPM columns that look like complication
   feeders by name but are sourced from a different domain — e.g. NSQIP).
6. Write `.complications_cpm_repoint_applied` marker file at repo root.
   Phase 7 of the build script checks for this marker before dropping
   deprecated tables.

PHI rule: research_id only. Logs only counts and column names.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from motherduck_client import get_token, token_mode  # noqa: E402

CANONICAL_DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"

CPM_SCHEMA = "main"
CPM_TABLE = "canonical_patient_master"
ROLLUP_SCHEMA = "main"
ROLLUP_TABLE = "canonical_complications_patient_rollup_v1"

MARKER_PATH = REPO_ROOT / ".complications_cpm_repoint_applied"

BUILD_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
RUN_TS_COMPACT = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = OUTPUT_DIR / f"364_cpm_repoint_run_{RUN_TS_COMPACT}.log"
DECISION_PATH = OUTPUT_DIR / f"364_cpm_repoint_decision_{RUN_TS_COMPACT}.json"

# ---------------------------------------------------------------------------
# Repoint mappings
# ---------------------------------------------------------------------------
#
# (cpm_col, rollup_col, rationale)
#
# Each CPM BOOLEAN column listed here is repointed to read from the named
# rollup column. The previous values came from complication_phenotype_v1
# / complication_patient_summary_v1 (which are about to be dropped).
# Existing CPM "*_confirmed" columns retain their semantics post-CHANGE G
# but are now sourced from the rollup's `ever_<type>_probable_or_better`
# (the publication default tier per Logan's spec). CPM consumers wanting
# the strict-definitive or wide any-evidence tier should read the new
# `comp_<type>_<tier>` columns added by ADD_EVIDENCE_TIERED below.
REPOINT: list[tuple[str, str, str]] = [
    ("any_confirmed_complication", "any_confirmed_complication_derived",
     "any complication present (computed from rollup row pattern)"),
    ("any_confirmed_complication_flag", "any_confirmed_complication_derived",
     "any complication present (alias of above)"),
    ("any_analysis_eligible_complication", "any_confirmed_complication_derived",
     "best-effort proxy: any present == eligible (was: phenotype "
     "analysis_eligible_flag)"),
    ("n_confirmed_complications", "n_complication_types_present",
     "rollup count of distinct types present (was: phenotype confirmed count)"),
    ("comp_rln_injury_confirmed", "ever_rln_injury_probable_or_better",
     "publication-default tier (CHANGE G)"),
    ("comp_hypocalcemia_confirmed", "ever_hypocalcemia_clinical_probable_or_better",
     "publication-default tier (CHANGE G)"),
    ("comp_hypoparathyroidism_confirmed", "ever_hypoparathyroidism_probable_or_better",
     "publication-default tier (CHANGE G)"),
    ("comp_hematoma_confirmed", "ever_hematoma_probable_or_better",
     "publication-default tier (CHANGE G)"),
    ("comp_seroma_confirmed", "ever_seroma_probable_or_better",
     "publication-default tier (CHANGE G)"),
    ("comp_chyle_leak_confirmed", "ever_chyle_leak_probable_or_better",
     "publication-default tier (CHANGE G)"),
    ("comp_wound_infection_confirmed", "ever_wound_infection_probable_or_better",
     "publication-default tier (CHANGE G)"),
    ("rln_injury_is_confirmed", "ever_rln_injury_probable_or_better",
     "publication-default tier (CHANGE G)"),
]

# CHANGE G — 36 new CPM BOOL columns mirror the rollup's tiered ever_*
# flags. Naming uses comp_<type>_<tier> to align with existing CPM
# convention while staying distinct from the legacy *_confirmed cols.
COMPLICATION_TYPES_FOR_CPM: tuple[str, ...] = (
    "rln_injury", "vocal_cord_paralysis", "hypocalcemia_clinical",
    "hypoparathyroidism", "hematoma", "seroma", "chyle_leak",
    "wound_infection", "pneumothorax", "airway_complication",
    "wound_dehiscence", "mortality",
)
TIERS_FOR_CPM: tuple[str, ...] = (
    "definitive", "probable_or_better", "any_evidence",
)
ADD_EVIDENCE_TIERED: list[tuple[str, str, str]] = [
    (f"comp_{ct}_{tier}", f"ever_{ct}_{tier}",
     f"CHANGE G tiered (rollup ever_{ct}_{tier})")
    for ct in COMPLICATION_TYPES_FOR_CPM
    for tier in TIERS_FOR_CPM
]

# CHANGE D — 8 NEW BOOL columns added to CPM, populated from the rollup's
# temporal classification flags. ALTER TABLE ADD COLUMN if missing; then
# UPDATE from rollup. Patients without a rollup row get FALSE.
ADD_TEMPORAL: list[tuple[str, str, str]] = [
    ("comp_hypoparathyroidism_preexisting",
     "hypoparathyroidism_preexisting",
     "evidence of hypopara before first surgery (CHANGE D)"),
    ("comp_hypoparathyroidism_new_postop",
     "hypoparathyroidism_new_postop",
     "first hypopara evidence on/after first surgery"),
    ("comp_hypoparathyroidism_transient",
     "hypoparathyroidism_transient",
     "new postop AND no evidence beyond 6 months post-op"),
    ("comp_hypoparathyroidism_permanent",
     "hypoparathyroidism_permanent",
     "new postop AND evidence persists at/beyond 6 months post-op"),
    ("comp_hypocalcemia_clinical_preexisting",
     "hypocalcemia_clinical_preexisting",
     "evidence of clinical hypocalcemia before first surgery (CHANGE D)"),
    ("comp_hypocalcemia_clinical_new_postop",
     "hypocalcemia_clinical_new_postop",
     "first clinical hypocalcemia evidence on/after first surgery"),
    ("comp_hypocalcemia_clinical_transient",
     "hypocalcemia_clinical_transient",
     "new postop AND no evidence beyond 6 months post-op"),
    ("comp_hypocalcemia_clinical_permanent",
     "hypocalcemia_clinical_permanent",
     "new postop AND evidence persists at/beyond 6 months post-op"),
]

# CPM columns that LOOK like they need repointing but don't (separate domain
# or non-BOOL feeder semantics). Logged in the audit report; not touched.
SKIP_HEURISTIC_FP: list[tuple[str, str]] = [
    ("nsqip_hypocalcemia", "NSQIP source — separate registry, not from "
     "the 5 deprecated source tables"),
    ("nsqip_hypocalcemia_event", "NSQIP source"),
    ("nsqip_hypocalcemia_event_type", "NSQIP source"),
    ("nsqip_hypocalcemia_flag", "NSQIP source"),
    ("nsqip_hypocalcemia_last_check", "NSQIP source"),
    ("nsqip_hypocalcemia_postdischarge", "NSQIP source"),
    ("nsqip_hypocalcemia_predischarge", "NSQIP source"),
    ("nsqip_hypocalcemia_recovered_flag", "NSQIP source"),
    ("nsqip_hypoparathyroidism_recovered_flag", "NSQIP source"),
    ("nsqip_hematoma_flag", "NSQIP source"),
    ("nsqip_neck_hematoma", "NSQIP source"),
    ("nsqip_rln_injury", "NSQIP source"),
    ("nsqip_rln_injury_flag", "NSQIP source"),
    ("nsqip_rln_monitoring", "NSQIP source"),
    ("prm_followup_has_complications", "PRM source — separate domain"),
    ("prm_hypocalcemia_lab_flag", "PRM source"),
    ("prm_hypoparathyroidism_lab_flag", "PRM source"),
    ("prm_rln_worst_grade", "PRM source"),
    ("syn_io_rln_monitoring", "synoptic source — separate domain"),
    ("ops_periop_complications", "operative-summary source — separate domain"),
    ("op_rln_monitoring_any", "operative source — separate domain"),
    ("mri_vocal_cords_described", "imaging-derived — not in scope"),
    ("mri_vocal_cords_normal", "imaging-derived — not in scope"),
    # *_status, comp_*_days_postop, comp_*_evidence_tier, comp_*_permanent,
    # comp_*_suspected, comp_*_timing_window, comp_*_transient,
    # comp_*_treatment_req, rln_classification, rln_injury_days_postop,
    # rln_injury_detection_date, rln_injury_evidence, rln_injury_tier,
    # rln_injury_type, rln_laterality, rln_permanent_flag, rln_status,
    # rln_temporality, rln_transient_flag, mortality_type,
    # nlp_ne_complications_*, op_nlp_intraop_complication_*,
    # op_nlp_rln_finding_* — these are NOT BOOL feeders; they encode
    # finer-grained metadata (status enum, timing, evidence tier, dates,
    # mention counts). Repointing them properly would require richer
    # derivations that don't fit the simple "set to rollup BOOL" pattern.
    # They stay pointed at the legacy table until the legacy table is
    # dropped (Step 7), at which point they become NULL. A future commit
    # can reconstruct them from the events table via deeper derivations
    # if downstream demand exists.
    ("(see audit report)", "non-BOOL feeders (status/timing/dates/counts) "
     "are NOT touched by this script — they will go NULL when the legacy "
     "tables drop in --phase 7"),
]


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}Z] {msg}", flush=True)


def fq(schema: str, name: str) -> str:
    return f'"{CANONICAL_DB}"."{schema}"."{name}"'


def fq_archive(name: str) -> str:
    return f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"."{name}"'


def connect() -> duckdb.DuckDBPyConnection:
    tok = get_token()
    if not tok:
        raise SystemExit(
            f"No MotherDuck RW token (token_mode={token_mode()}). "
            "Set MD_SA_TOKEN / MOTHERDUCK_TOKEN or motherduck.local.toml."
        )
    log(f"Connecting md:{CANONICAL_DB} (token_mode={token_mode()})")
    con = duckdb.connect(f"md:{CANONICAL_DB}?motherduck_token={tok}")
    con.execute(f'USE "{CANONICAL_DB}"')
    return con


def column_exists(con: duckdb.DuckDBPyConnection, schema: str,
                  table: str, column: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_catalog=? AND table_schema=? AND table_name=? "
        "AND column_name=?",
        [CANONICAL_DB, schema, table, column],
    ).fetchone()
    return row is not None


def true_count(con: duckdb.DuckDBPyConnection, schema: str,
               table: str, col: str) -> tuple[int, int, int]:
    if not column_exists(con, schema, table, col):
        return (-1, -1, -1)
    row = con.execute(
        f"SELECT "
        f"  SUM(CASE WHEN \"{col}\"=TRUE THEN 1 ELSE 0 END), "
        f"  SUM(CASE WHEN \"{col}\"=FALSE THEN 1 ELSE 0 END), "
        f"  SUM(CASE WHEN \"{col}\" IS NULL THEN 1 ELSE 0 END) "
        f"FROM {fq(schema, table)}"
    ).fetchone()
    return (int(row[0] or 0), int(row[1] or 0), int(row[2] or 0))


def step_1_snapshot(
    con: duckdb.DuckDBPyConnection, do_writes: bool,
) -> str | None:
    log("=" * 78)
    log("STEP 1 — Snapshot CPM pre-state to archive")
    log("=" * 78)
    snapshot_name = f"{CPM_TABLE}_pre364cpmrepoint_{BUILD_TS}"
    n_pre = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(CPM_SCHEMA, CPM_TABLE)}"
    ).fetchone()[0])
    log(f"  CPM has {n_pre:,} rows")
    log(f"  snapshot plan: {CPM_SCHEMA}.{CPM_TABLE} -> {snapshot_name}")
    if not do_writes:
        return None
    con.execute(f'USE "{ARCHIVE_DB}"')
    existing = con.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema=? AND table_name=?",
        [ARCHIVE_SCHEMA, snapshot_name],
    ).fetchone()
    con.execute(f'USE "{CANONICAL_DB}"')
    if existing:
        log(f"  snapshot already exists: {snapshot_name} — skipping")
        return snapshot_name
    con.execute(
        f"CREATE TABLE {fq_archive(snapshot_name)} AS "
        f"SELECT * FROM {fq(CPM_SCHEMA, CPM_TABLE)}"
    )
    n_dst = int(con.execute(
        f"SELECT COUNT(*) FROM {fq_archive(snapshot_name)}"
    ).fetchone()[0])
    if n_dst != n_pre:
        raise RuntimeError(
            f"CPM snapshot mismatch: src={n_pre} dst={n_dst}"
        )
    log(f"  snapshot created -> {snapshot_name} ({n_dst:,} rows)")
    return snapshot_name


def step_2_validate(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 2 — Validate dependencies")
    log("=" * 78)

    # Rollup must exist; verify required cols.
    if not column_exists(con, ROLLUP_SCHEMA, ROLLUP_TABLE, "research_id"):
        raise SystemExit(
            f"Rollup {ROLLUP_SCHEMA}.{ROLLUP_TABLE} missing or has no "
            f"research_id column. Run scripts/364_complications_consolidation.py "
            f"--commit first."
        )
    needed = {rc for _, rc, _ in REPOINT
              if rc != "any_confirmed_complication_derived"}
    missing = [c for c in needed
               if not column_exists(con, ROLLUP_SCHEMA, ROLLUP_TABLE, c)]
    if missing:
        raise SystemExit(
            f"Rollup is missing columns required for repoint: {missing}. "
            f"Was the build complete?"
        )
    log(f"  rollup {ROLLUP_TABLE} has all {len(needed)} required cols ✓")

    # CPM must exist with research_id.
    if not column_exists(con, CPM_SCHEMA, CPM_TABLE, "research_id"):
        raise SystemExit(
            f"{CPM_SCHEMA}.{CPM_TABLE} missing or no research_id col."
        )

    n_cpm = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(CPM_SCHEMA, CPM_TABLE)}"
    ).fetchone()[0])
    n_rollup = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(ROLLUP_SCHEMA, ROLLUP_TABLE)}"
    ).fetchone()[0])
    n_overlap = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(CPM_SCHEMA, CPM_TABLE)} cpm "
        f"WHERE EXISTS (SELECT 1 FROM {fq(ROLLUP_SCHEMA, ROLLUP_TABLE)} r "
        f"WHERE CAST(r.research_id AS VARCHAR) = CAST(cpm.research_id AS VARCHAR))"
    ).fetchone()[0])
    log(f"  CPM rows={n_cpm:,}; rollup rows={n_rollup:,}; "
        f"overlapping research_ids={n_overlap:,}")
    if n_overlap != n_cpm:
        log(f"  WARNING: only {n_overlap:,}/{n_cpm:,} CPM rows have a rollup match")
    return {"n_cpm": n_cpm, "n_rollup": n_rollup, "n_overlap": n_overlap}


def step_3_repoint(
    con: duckdb.DuckDBPyConnection, do_writes: bool,
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 3 — Repoint CPM BOOL cols to canonical_complications_patient_rollup_v1")
    log("=" * 78)
    deltas: list[dict[str, Any]] = []

    # Define a derived expression for any_confirmed_complication that uses
    # the rollup's existing columns. We don't want to add a column to the
    # rollup itself; just derive it inline here.
    # Definition: TRUE iff n_complication_types_present > 0.
    derived_def = "(COALESCE(r.n_complication_types_present, 0) > 0)"
    derived_label = "any_confirmed_complication_derived"

    for cpm_col, rollup_col, rationale in REPOINT:
        if not column_exists(con, CPM_SCHEMA, CPM_TABLE, cpm_col):
            log(f"  REPOINT skip: CPM col {cpm_col!r} does not exist (will not "
                f"add it — schema-stable behavior)")
            continue

        n_t_pre, n_f_pre, n_n_pre = true_count(
            con, CPM_SCHEMA, CPM_TABLE, cpm_col)
        log(f"  pre  {cpm_col:42s}: TRUE={n_t_pre:,} "
            f"FALSE={n_f_pre:,} NULL={n_n_pre:,}")

        # Choose the read expression.
        if rollup_col == derived_label:
            read_expr = derived_def
        else:
            read_expr = f"r.\"{rollup_col}\""

        # Different SQL for BOOLEAN target vs BIGINT/INTEGER (e.g.
        # n_confirmed_complications). Probe target type from
        # information_schema.columns.
        dtype_row = con.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_catalog=? AND table_schema=? AND table_name=? "
            "AND column_name=?",
            [CANONICAL_DB, CPM_SCHEMA, CPM_TABLE, cpm_col],
        ).fetchone()
        cpm_dtype = (dtype_row[0] if dtype_row else "").upper()
        # For BOOLEAN: COALESCE(read_expr, FALSE).
        # For numeric: COALESCE(read_expr, 0).
        if "BOOL" in cpm_dtype:
            coalesce_default = "FALSE"
        elif any(t in cpm_dtype for t in ("BIGINT", "INTEGER", "INT", "DECIMAL", "DOUBLE")):
            coalesce_default = "0"
        else:
            log(f"    REPOINT abort: unhandled CPM dtype {cpm_dtype!r} "
                f"for {cpm_col} — leaving untouched")
            deltas.append({"cpm_col": cpm_col, "rollup_col": rollup_col,
                           "applied": False,
                           "reason": f"unhandled dtype {cpm_dtype}"})
            continue

        if not do_writes:
            log(f"    [dry-run] would UPDATE {cpm_col} ← "
                f"COALESCE({read_expr}, {coalesce_default})")
            deltas.append({"cpm_col": cpm_col, "rollup_col": rollup_col,
                           "applied": False,
                           "pre_true": n_t_pre, "pre_false": n_f_pre,
                           "pre_null": n_n_pre})
            continue

        # Pull values from rollup (matching on research_id).
        con.execute(
            f"UPDATE {fq(CPM_SCHEMA, CPM_TABLE)} AS cpm "
            f"SET \"{cpm_col}\" = COALESCE({read_expr}, {coalesce_default}) "
            f"FROM {fq(ROLLUP_SCHEMA, ROLLUP_TABLE)} AS r "
            f"WHERE CAST(r.research_id AS VARCHAR) = CAST(cpm.research_id AS VARCHAR)"
        )
        # Patients without a rollup row default to FALSE/0.
        con.execute(
            f"UPDATE {fq(CPM_SCHEMA, CPM_TABLE)} AS cpm "
            f"SET \"{cpm_col}\" = {coalesce_default} "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM {fq(ROLLUP_SCHEMA, ROLLUP_TABLE)} r "
            f"  WHERE CAST(r.research_id AS VARCHAR) = CAST(cpm.research_id AS VARCHAR)"
            f")"
        )
        n_t_post, n_f_post, n_n_post = true_count(
            con, CPM_SCHEMA, CPM_TABLE, cpm_col)
        log(f"  post {cpm_col:42s}: TRUE={n_t_post:,} "
            f"FALSE={n_f_post:,} NULL={n_n_post:,}  "
            f"(Δ TRUE: {n_t_post - n_t_pre:+,})")
        deltas.append({"cpm_col": cpm_col, "rollup_col": rollup_col,
                       "applied": True,
                       "pre_true": n_t_pre, "post_true": n_t_post,
                       "delta_true": n_t_post - n_t_pre,
                       "rationale": rationale})
    return {"repointed": deltas}


def _add_and_populate_bool_cols(
    con: duckdb.DuckDBPyConnection,
    do_writes: bool,
    spec: list[tuple[str, str, str]],
    label: str,
) -> list[dict[str, Any]]:
    """Generic ALTER TABLE ADD COLUMN BOOLEAN + populate-from-rollup helper.

    Each `spec` entry is (cpm_col, rollup_col, rationale). Idempotent —
    skips ADD COLUMN if the column already exists, then re-populates from
    the rollup. Patients without a rollup row default to FALSE.
    """
    deltas: list[dict[str, Any]] = []
    for cpm_col, rollup_col, rationale in spec:
        already = column_exists(con, CPM_SCHEMA, CPM_TABLE, cpm_col)
        if already:
            n_t, n_f, n_n = true_count(con, CPM_SCHEMA, CPM_TABLE, cpm_col)
            log(f"  ADD skip: CPM col {cpm_col!r} already exists "
                f"(TRUE={n_t:,} FALSE={n_f:,} NULL={n_n:,}) — repointing")
        else:
            log(f"  ADD plan: ALTER TABLE ADD COLUMN {cpm_col} BOOLEAN  "
                f"-- {rationale}")
            if not do_writes:
                deltas.append({"cpm_col": cpm_col, "rollup_col": rollup_col,
                               "applied": False, "step": "add_column"})
                continue
            con.execute(
                f"ALTER TABLE {fq(CPM_SCHEMA, CPM_TABLE)} "
                f"ADD COLUMN \"{cpm_col}\" BOOLEAN"
            )
        # Populate from rollup.
        if not do_writes:
            log(f"    [dry-run] would UPDATE {cpm_col} ← "
                f"COALESCE(r.{rollup_col}, FALSE)")
            deltas.append({"cpm_col": cpm_col, "rollup_col": rollup_col,
                           "applied": False, "step": "populate"})
            continue
        if not column_exists(con, ROLLUP_SCHEMA, ROLLUP_TABLE, rollup_col):
            log(f"    REPOINT abort: rollup col {rollup_col!r} missing — "
                f"set CPM col to NULL")
            deltas.append({"cpm_col": cpm_col, "rollup_col": rollup_col,
                           "applied": False, "step": "missing_rollup"})
            continue
        con.execute(
            f"UPDATE {fq(CPM_SCHEMA, CPM_TABLE)} AS cpm "
            f"SET \"{cpm_col}\" = COALESCE(r.\"{rollup_col}\", FALSE) "
            f"FROM {fq(ROLLUP_SCHEMA, ROLLUP_TABLE)} AS r "
            f"WHERE CAST(r.research_id AS VARCHAR) = "
            f"      CAST(cpm.research_id AS VARCHAR)"
        )
        con.execute(
            f"UPDATE {fq(CPM_SCHEMA, CPM_TABLE)} AS cpm "
            f"SET \"{cpm_col}\" = FALSE "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM {fq(ROLLUP_SCHEMA, ROLLUP_TABLE)} r "
            f"  WHERE CAST(r.research_id AS VARCHAR) = "
            f"        CAST(cpm.research_id AS VARCHAR)"
            f")"
        )
        n_t, n_f, n_n = true_count(con, CPM_SCHEMA, CPM_TABLE, cpm_col)
        log(f"  populated {cpm_col}: TRUE={n_t:,} FALSE={n_f:,} NULL={n_n:,} "
            f"({label})")
        deltas.append({"cpm_col": cpm_col, "rollup_col": rollup_col,
                       "applied": True, "post_true": n_t, "label": label})
    return deltas


def step_3b_add_temporal_cols(
    con: duckdb.DuckDBPyConnection, do_writes: bool,
) -> dict[str, Any]:
    """CHANGE D — 8 temporal-classification BOOL cols.
    CHANGE G — 36 evidence-tiered BOOL cols (12 types × 3 tiers).
    Both are added via the generic _add_and_populate_bool_cols helper."""
    log("=" * 78)
    log("STEP 3B — Add CHANGE D temporal + CHANGE G tiered BOOL cols to CPM")
    log("=" * 78)
    log(f"  CHANGE D temporal cols: {len(ADD_TEMPORAL)}")
    deltas_d = _add_and_populate_bool_cols(
        con, do_writes, ADD_TEMPORAL, "CHANGE_D_temporal"
    )
    log(f"  CHANGE G tiered cols: {len(ADD_EVIDENCE_TIERED)}")
    deltas_g = _add_and_populate_bool_cols(
        con, do_writes, ADD_EVIDENCE_TIERED, "CHANGE_G_tiered"
    )
    return {"added_temporal": deltas_d, "added_evidence_tiered": deltas_g}


def step_4_log_skips(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 4 — Heuristic-FP skips (NOT touched, documented)")
    log("=" * 78)
    skipped: list[dict[str, str]] = []
    for cpm_col, reason in SKIP_HEURISTIC_FP:
        present = (
            column_exists(con, CPM_SCHEMA, CPM_TABLE, cpm_col)
            if not cpm_col.startswith("(") else None
        )
        log(f"  SKIP {cpm_col} ({'present' if present else 'absent/n-a'}): "
            f"{reason}")
        skipped.append({"cpm_col": cpm_col, "present": str(present),
                        "reason": reason})
    return {"skipped": skipped}


def step_5_marker(do_writes: bool) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 5 — Write Step-7 safety-gate marker")
    log("=" * 78)
    log(f"  marker plan: {MARKER_PATH}")
    if not do_writes:
        return {"marker_written": False}
    MARKER_PATH.write_text(
        f"Complications CPM feeder repoint applied at {RUN_DATE} {BUILD_TS}\n"
        f"by scripts/364_cpm_feeder_repoint.py.\n"
        f"\n"
        f"This marker satisfies the Pre-Strip Safety Gate in\n"
        f"scripts/364_complications_consolidation.py --phase 7. Do NOT\n"
        f"delete this file until the cascade strip has been applied AND\n"
        f"verified.\n"
        f"\n"
        f"Repointed BOOL/numeric CPM cols: {len(REPOINT)}\n"
        f"Documented skips (heuristic FPs): {len(SKIP_HEURISTIC_FP)}\n",
        encoding="utf-8",
    )
    log(f"  marker written -> {MARKER_PATH}")
    return {"marker_written": True, "marker_path": str(MARKER_PATH)}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repoint CPM complication feeders to "
                    "canonical_complications_patient_rollup_v1."
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--commit", action="store_true",
                      help="Apply changes.")
    mode.add_argument("--dry-run", action="store_true",
                      help="Plan only — print intended SQL, no writes.")
    args = parser.parse_args()

    do_writes = bool(args.commit)
    log(f"Run config: do_writes={do_writes} BUILD_TS={BUILD_TS}")

    con = connect()
    snapshot = step_1_snapshot(con, do_writes)
    deps = step_2_validate(con)
    s3 = step_3_repoint(con, do_writes)
    s3b = step_3b_add_temporal_cols(con, do_writes)
    s4 = step_4_log_skips(con)
    s5 = step_5_marker(do_writes)

    log("=" * 78)
    log("DONE — CPM feeder repoint complete"
        + (" (dry-run)" if not do_writes else " and committed"))
    log(f"  Snapshot: {snapshot}")
    log(f"  Repointed: {len(REPOINT)} mapped CPM cols")
    log(f"  Added temporal cols (CHANGE D): {len(ADD_TEMPORAL)}")
    log(f"  Added evidence-tiered cols (CHANGE G): {len(ADD_EVIDENCE_TIERED)}")
    log(f"  Documented skips (heuristic FPs): {len(SKIP_HEURISTIC_FP)}")
    log(f"  Marker: {s5.get('marker_path', 'not written (dry-run)')}")
    log(f"  CPM rows: {deps['n_cpm']:,}; "
        f"rollup rows: {deps['n_rollup']:,}; "
        f"overlap: {deps['n_overlap']:,}")

    DECISION_PATH.write_text(json.dumps({
        "build_ts": BUILD_TS, "do_writes": do_writes,
        "snapshot": snapshot, "deps": deps,
        "step_3": s3, "step_3b": s3b, "step_4": s4, "step_5": s5,
    }, indent=2, default=str))
    log(f"  decision log: {DECISION_PATH.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
