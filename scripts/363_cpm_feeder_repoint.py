#!/usr/bin/env python3
"""
Script 363 — CPM feeder repoint to canonical_invasion_patient_rollup_v1.

Repoints canonical_patient_master invasion-related BOOL columns to
read from the v3 invasion canonical instead of the about-to-be-stripped
operative BOOL flags. Runs between the v3-iter-2 build commit
(`b0a03b0`) and the cascade strip (`--commit --phase 7`).

REPOINT (8 existing CPM BOOL cols updated from rollup):
    nlp_path_ete_mentioned          ← any_gross_ete_anywhere
    op_esophageal_inv_any            ← any_esophageal_anywhere
    op_intraop_gross_ete_any         ← any_gross_ete_anywhere
    op_local_invasion_any            ← any_soft_tissue_anywhere
                                       (v3 routing: local_invasion_flag
                                        was rerouted to soft_tissue
                                        invasion_type)
    op_nlp_esophageal_involvement    ← any_esophageal_anywhere
                                       (heuristic mismap fix: was
                                        wrongly pointed at tracheal in
                                        the original plan)
    op_nlp_gross_invasion            ← any_gross_ete_anywhere
    op_nlp_tracheal_involvement      ← any_tracheal_anywhere
    op_tracheal_inv_any              ← any_tracheal_anywhere

ADD (7 new CPM BOOL cols, populated from rollup):
    any_vascular_microscopic_anywhere
    any_lymphatic_microscopic_anywhere
    any_capsular_anywhere
    any_perineural_anywhere
    any_soft_tissue_anywhere
    any_microscopic_ete_anywhere
    any_airway_anywhere

EXPLICIT SKIPS (8 cols flagged in heuristic plan but NOT real feeders):
    nlp_tg_undetectable_mentioned   (thyroglobulin lab marker — wrong
                                     domain; heuristic false positive)
    op_nlp_esophageal_n_mentions    (BIGINT entity-count from
                                     note_entities, not BOOL feeder)
    op_nlp_tracheal_n_mentions      (BIGINT, same)
    op_esophageal_inv_first_date    (DATE metadata from note_entities)
    op_esophageal_inv_first_source_note_ref   (VARCHAR metadata)
    op_esophageal_inv_first_evidence_text     (VARCHAR metadata)
    op_esophageal_inv_source_table            (VARCHAR metadata)
    op_esophageal_inv_n_notes_documenting     (INTEGER count)

These 8 do NOT have a direct dependency on the BOOL flags that
Step 7 will drop. They are derived from joining note_entities tables
and remain unaffected by the cascade strip.

Side effect: writes marker file `.invasion_cpm_repoint_applied` at
repo root. Step 7 of `scripts/363_invasion_canonical.py` checks for
this marker as Pre-Strip Safety Gate 7.2.4 (CPM feeder repoint
applied).

Usage::

    python scripts/363_cpm_feeder_repoint.py --dry-run
    python scripts/363_cpm_feeder_repoint.py --commit

PHI rule: research_id only. Logs only counts and column names.
"""
from __future__ import annotations

import argparse
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
ROLLUP_TABLE = "canonical_invasion_patient_rollup_v1"

MARKER_PATH = REPO_ROOT / ".invasion_cpm_repoint_applied"

BUILD_TS = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ---------------------------------------------------------------------------
# Repoint mappings
# ---------------------------------------------------------------------------

# (cpm_col, rollup_col, rationale)
REPOINT: list[tuple[str, str, str]] = [
    ("nlp_path_ete_mentioned",
     "any_gross_ete_anywhere",
     "NLP-extracted path ETE → cross-modal gross ETE present"),
    ("op_esophageal_inv_any",
     "any_esophageal_anywhere",
     "operative esophageal invasion any → cross-modal"),
    ("op_intraop_gross_ete_any",
     "any_gross_ete_anywhere",
     "intra-op gross ETE any → cross-modal"),
    ("op_local_invasion_any",
     "any_soft_tissue_anywhere",
     "v3 routing: local_invasion_flag → soft_tissue invasion_type"),
    ("op_nlp_esophageal_involvement",
     "any_esophageal_anywhere",
     "NLP esophageal involvement → cross-modal "
     "(corrected heuristic mismap to tracheal)"),
    ("op_nlp_gross_invasion",
     "any_gross_ete_anywhere",
     "NLP gross invasion → cross-modal gross ETE present"),
    ("op_nlp_tracheal_involvement",
     "any_tracheal_anywhere",
     "NLP tracheal involvement → cross-modal"),
    ("op_tracheal_inv_any",
     "any_tracheal_anywhere",
     "operative tracheal invasion any → cross-modal"),
]

# (cpm_col_to_add, rollup_col, rationale)
ADD: list[tuple[str, str, str]] = [
    ("any_vascular_microscopic_anywhere",
     "any_vascular_microscopic_anywhere",
     "v3 vocab: microscopic vascular invasion (V only — split from "
     "v2 V/L bundle)"),
    ("any_lymphatic_microscopic_anywhere",
     "any_lymphatic_microscopic_anywhere",
     "v3 NEW: lymphatic invasion (split from v2 V/L bundle)"),
    ("any_capsular_anywhere",
     "any_capsular_anywhere",
     "v3 NEW: capsular invasion (split from v2 'local' bundle)"),
    ("any_perineural_anywhere",
     "any_perineural_anywhere",
     "v3 NEW: perineural invasion (split from v2 'local' bundle)"),
    ("any_soft_tissue_anywhere",
     "any_soft_tissue_anywhere",
     "v3 NEW: soft tissue invasion (split from v2 'local' bundle; "
     "also receives op_note local_invasion_flag rerouted)"),
    ("any_microscopic_ete_anywhere",
     "any_microscopic_ete_anywhere",
     "v3 ETE subtype: microscopic_ete (separate from gross_ete; "
     "disambiguated via entity_value modifier in LLM CTE)"),
    ("any_airway_anywhere",
     "any_airway_anywhere",
     "v3 vocab: direct airway invasion (NOT deviation/compression "
     "— mass-effect entities EXCISED per Pattern 15)"),
]

# Heuristic false-positives explicitly NOT touched by this script.
SKIP_HEURISTIC_FP: list[tuple[str, str]] = [
    ("nlp_tg_undetectable_mentioned",
     "thyroglobulin lab marker — wrong domain (heuristic false positive)"),
    ("op_nlp_esophageal_n_mentions",
     "BIGINT entity count from note_entities; not a BOOL feeder"),
    ("op_nlp_tracheal_n_mentions",
     "BIGINT entity count from note_entities; not a BOOL feeder"),
    ("op_esophageal_inv_first_date",
     "DATE metadata from note_entities join; not BOOL-flag dependent"),
    ("op_esophageal_inv_first_source_note_ref",
     "VARCHAR metadata from note_entities; not BOOL-flag dependent"),
    ("op_esophageal_inv_first_evidence_text",
     "VARCHAR evidence text from note_entities; not BOOL-flag dependent"),
    ("op_esophageal_inv_source_table",
     "VARCHAR source-table tag from note_entities; not BOOL-flag dependent"),
    ("op_esophageal_inv_n_notes_documenting",
     "INTEGER note count from note_entities; not BOOL-flag dependent"),
]


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}Z] {msg}", flush=True)


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


def fq(schema: str, name: str) -> str:
    return f'"{CANONICAL_DB}"."{schema}"."{name}"'


def fq_archive(name: str) -> str:
    return f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"."{name}"'


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
    """Return (n_true, n_false, n_null)."""
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


def step_1_snapshot_pre_state(
    con: duckdb.DuckDBPyConnection, do_writes: bool,
) -> str | None:
    """Snapshot pre-state of CPM to archive_pub_v1_0 as a parity guard."""
    log("=" * 78)
    log("STEP 1 — Snapshot CPM pre-state to archive")
    log("=" * 78)
    snapshot_name = f"{CPM_TABLE}_pre363cpmrepoint_{BUILD_TS}"
    n_pre = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(CPM_SCHEMA, CPM_TABLE)}"
    ).fetchone()[0])
    log(f"  CPM has {n_pre:,} rows")
    log(f"  snapshot plan: {CPM_SCHEMA}.{CPM_TABLE} -> {snapshot_name}")
    if not do_writes:
        return None
    # Idempotent
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


def step_2_validate_dependencies(
    con: duckdb.DuckDBPyConnection,
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 2 — Validate dependencies")
    log("=" * 78)
    # Rollup must exist with all the cols we plan to read
    rollup_cols_needed = set(rc for _, rc, _ in REPOINT) | set(rc for _, rc, _ in ADD)
    missing = [
        rc for rc in rollup_cols_needed
        if not column_exists(con, ROLLUP_SCHEMA, ROLLUP_TABLE, rc)
    ]
    if missing:
        raise SystemExit(
            f"Rollup is missing columns required for repoint: {missing}. "
            f"Run scripts/363_invasion_canonical.py --commit --skip-strip "
            f"first."
        )
    log(f"  rollup {ROLLUP_TABLE} has all "
        f"{len(rollup_cols_needed)} required cols")
    # CPM must exist
    if not column_exists(con, CPM_SCHEMA, CPM_TABLE, "research_id"):
        raise SystemExit(
            f"{CPM_SCHEMA}.{CPM_TABLE} missing or no research_id col."
        )
    log(f"  {CPM_SCHEMA}.{CPM_TABLE} exists with research_id")
    # CPM rows == rollup rows? (informational only)
    n_cpm = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(CPM_SCHEMA, CPM_TABLE)}"
    ).fetchone()[0])
    n_rollup = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(ROLLUP_SCHEMA, ROLLUP_TABLE)}"
    ).fetchone()[0])
    n_overlap = int(con.execute(
        f"SELECT COUNT(*) FROM {fq(CPM_SCHEMA, CPM_TABLE)} cpm "
        f"WHERE EXISTS (SELECT 1 FROM {fq(ROLLUP_SCHEMA, ROLLUP_TABLE)} r "
        f"WHERE r.research_id = cpm.research_id)"
    ).fetchone()[0])
    log(f"  CPM rows={n_cpm:,}; rollup rows={n_rollup:,}; "
        f"overlapping research_ids={n_overlap:,}")
    return {"n_cpm": n_cpm, "n_rollup": n_rollup, "n_overlap": n_overlap}


def step_3_repoint_existing(
    con: duckdb.DuckDBPyConnection, do_writes: bool,
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 3 — Repoint existing CPM BOOL cols")
    log("=" * 78)
    deltas: list[dict[str, Any]] = []
    for cpm_col, rollup_col, rationale in REPOINT:
        if not column_exists(con, CPM_SCHEMA, CPM_TABLE, cpm_col):
            log(f"  REPOINT skip: CPM col {cpm_col!r} does not exist")
            continue
        n_t_pre, n_f_pre, n_n_pre = true_count(
            con, CPM_SCHEMA, CPM_TABLE, cpm_col)
        log(f"  pre {cpm_col}: TRUE={n_t_pre:,} "
            f"FALSE={n_f_pre:,} NULL={n_n_pre:,}")
        if not do_writes:
            log(f"    [dry-run] would UPDATE {cpm_col} ← "
                f"COALESCE({rollup_col}, FALSE)  -- {rationale}")
            deltas.append({"cpm_col": cpm_col, "rollup_col": rollup_col,
                           "applied": False,
                           "pre_true": n_t_pre, "pre_false": n_f_pre,
                           "pre_null": n_n_pre})
            continue
        con.execute(
            f"UPDATE {fq(CPM_SCHEMA, CPM_TABLE)} AS cpm "
            f"SET \"{cpm_col}\" = COALESCE(r.\"{rollup_col}\", FALSE) "
            f"FROM {fq(ROLLUP_SCHEMA, ROLLUP_TABLE)} AS r "
            f"WHERE cpm.research_id = r.research_id"
        )
        # CPM rows that don't have a rollup match → set to FALSE
        con.execute(
            f"UPDATE {fq(CPM_SCHEMA, CPM_TABLE)} AS cpm "
            f"SET \"{cpm_col}\" = FALSE "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM {fq(ROLLUP_SCHEMA, ROLLUP_TABLE)} r "
            f"  WHERE r.research_id = cpm.research_id"
            f")"
        )
        n_t_post, n_f_post, n_n_post = true_count(
            con, CPM_SCHEMA, CPM_TABLE, cpm_col)
        log(f"  post {cpm_col}: TRUE={n_t_post:,} "
            f"FALSE={n_f_post:,} NULL={n_n_post:,} "
            f"  (Δ TRUE: {n_t_post - n_t_pre:+,})")
        deltas.append({"cpm_col": cpm_col, "rollup_col": rollup_col,
                       "applied": True,
                       "pre_true": n_t_pre, "post_true": n_t_post,
                       "delta_true": n_t_post - n_t_pre})
    return {"repointed": deltas}


def step_4_add_new_cols(
    con: duckdb.DuckDBPyConnection, do_writes: bool,
) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 4 — Add new CPM BOOL cols + populate from rollup")
    log("=" * 78)
    deltas: list[dict[str, Any]] = []
    for cpm_col, rollup_col, rationale in ADD:
        already = column_exists(con, CPM_SCHEMA, CPM_TABLE, cpm_col)
        if already:
            log(f"  ADD skip: CPM col {cpm_col!r} already exists "
                f"(idempotent)")
            n_t, n_f, n_n = true_count(con, CPM_SCHEMA, CPM_TABLE, cpm_col)
            deltas.append({"cpm_col": cpm_col, "rollup_col": rollup_col,
                           "applied": False, "already_existed": True,
                           "post_true": n_t})
            continue
        log(f"  ADD plan: ALTER TABLE ADD COLUMN {cpm_col} BOOLEAN "
            f"← {rollup_col}  -- {rationale}")
        if not do_writes:
            deltas.append({"cpm_col": cpm_col, "rollup_col": rollup_col,
                           "applied": False, "already_existed": False})
            continue
        con.execute(
            f"ALTER TABLE {fq(CPM_SCHEMA, CPM_TABLE)} "
            f"ADD COLUMN \"{cpm_col}\" BOOLEAN"
        )
        # Populate from rollup
        con.execute(
            f"UPDATE {fq(CPM_SCHEMA, CPM_TABLE)} AS cpm "
            f"SET \"{cpm_col}\" = COALESCE(r.\"{rollup_col}\", FALSE) "
            f"FROM {fq(ROLLUP_SCHEMA, ROLLUP_TABLE)} AS r "
            f"WHERE cpm.research_id = r.research_id"
        )
        # CPM rows that don't have a rollup match → FALSE
        con.execute(
            f"UPDATE {fq(CPM_SCHEMA, CPM_TABLE)} AS cpm "
            f"SET \"{cpm_col}\" = FALSE "
            f"WHERE NOT EXISTS ("
            f"  SELECT 1 FROM {fq(ROLLUP_SCHEMA, ROLLUP_TABLE)} r "
            f"  WHERE r.research_id = cpm.research_id"
            f")"
        )
        n_t, n_f, n_n = true_count(con, CPM_SCHEMA, CPM_TABLE, cpm_col)
        log(f"  added + populated {cpm_col}: "
            f"TRUE={n_t:,} FALSE={n_f:,} NULL={n_n:,}")
        deltas.append({"cpm_col": cpm_col, "rollup_col": rollup_col,
                       "applied": True, "post_true": n_t})
    return {"added": deltas}


def step_5_log_skips(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 5 — Heuristic false-positives (NOT touched, documented)")
    log("=" * 78)
    skipped: list[dict[str, str]] = []
    for cpm_col, reason in SKIP_HEURISTIC_FP:
        present = column_exists(con, CPM_SCHEMA, CPM_TABLE, cpm_col)
        log(f"  SKIP {cpm_col} ({'present' if present else 'absent'}): "
            f"{reason}")
        skipped.append({"cpm_col": cpm_col, "present": present,
                        "reason": reason})
    return {"skipped": skipped}


def step_6_marker(do_writes: bool) -> dict[str, Any]:
    log("=" * 78)
    log("STEP 6 — Write Step-7 safety-gate marker")
    log("=" * 78)
    log(f"  marker plan: {MARKER_PATH}")
    if not do_writes:
        return {"marker_written": False}
    MARKER_PATH.write_text(
        f"CPM feeder repoint applied at {RUN_DATE} {BUILD_TS}\n"
        f"by scripts/363_cpm_feeder_repoint.py.\n"
        f"\n"
        f"This marker satisfies Pre-Strip Safety Gate 7.2.4 in\n"
        f"scripts/363_invasion_canonical.py. Do NOT delete this file\n"
        f"until the cascade strip (--commit --phase 7) has been\n"
        f"applied AND verified.\n"
        f"\n"
        f"Repointed: {len(REPOINT)} existing CPM BOOL cols\n"
        f"Added: {len(ADD)} new CPM BOOL cols\n"
        f"Skipped: {len(SKIP_HEURISTIC_FP)} heuristic false-positives\n",
        encoding="utf-8",
    )
    log(f"  marker written -> {MARKER_PATH}")
    return {"marker_written": True, "marker_path": str(MARKER_PATH)}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Repoint CPM invasion feeders to "
                    "canonical_invasion_patient_rollup_v1."
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
    snapshot = step_1_snapshot_pre_state(con, do_writes)
    deps = step_2_validate_dependencies(con)
    step_3_repoint_existing(con, do_writes)
    step_4_add_new_cols(con, do_writes)
    step_5_log_skips(con)
    s6 = step_6_marker(do_writes)

    log("=" * 78)
    log("DONE — CPM feeder repoint complete"
        + (" (dry-run)" if not do_writes else " and committed"))
    log(f"  Snapshot: {snapshot}")
    log(f"  Repointed: {len(REPOINT)} existing BOOL cols")
    log(f"  Added: {len(ADD)} new BOOL cols")
    log(f"  Skipped (heuristic FP): {len(SKIP_HEURISTIC_FP)} cols")
    log(f"  Marker: {s6.get('marker_path', 'not written (dry-run)')}")
    log(f"  CPM rows: {deps['n_cpm']:,}; "
        f"rollup rows: {deps['n_rollup']:,}; "
        f"overlap: {deps['n_overlap']:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
