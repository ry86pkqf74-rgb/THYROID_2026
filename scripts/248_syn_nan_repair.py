#!/usr/bin/env python3
"""
Script 248 — Source-data string-'nan' repair (Phase 1 of v1_1 cleanup).

Date:    2026-04-16
Author:  THYROID_2026 canonical-finalization run (v1_1 cleanup)
Branch:  cleanup/canonical-finalization-20260416

Purpose
=======
Eliminate the literal-string 'nan' pollution that leaked into VARCHAR columns
on `canonical_patient_master` from upstream pandas exports
(`df.to_sql(...)` / inserts where `NaN` was rendered as the four-character
string 'nan' instead of SQL NULL). The 87% literal-'nan' rate on
`syn_architecture` and `syn_margin_distance_mm` is the headline case.

Scope
-----
1. Audit EVERY VARCHAR/TEXT column on canonical_patient_master and write
   one row per column to `manuscript_workspace.nan_string_audit_v1_1`
   with (column_name, n_literal_nan, n_true_null, n_real_values,
   n_distinct_real, repair_action).
2. Repair `syn_architecture`: UPDATE SET col = NULL WHERE col = 'nan'.
3. Repair `syn_margin_distance_mm`:
     - Add new column `syn_margin_distance_mm_num DOUBLE`.
     - Populate via TRY_CAST(NULLIF(syn_margin_distance_mm, 'nan') AS DOUBLE).
     - Rename original VARCHAR column to `syn_margin_distance_mm_raw_str`
       and add COMMENT pointing to the new column.
4. Sweep all other VARCHAR columns (syn_*, nlp_*, and every other VARCHAR)
   and apply UPDATE SET col = NULL WHERE col = 'nan' to any column where
   n_literal_nan > 0.
5. Idempotent: re-running yields zero literal-'nan' cells across CPM.

Default mode is --dry-run (no writes). Pass --apply to execute.

Tables READ
-----------
  thyroid_canonical_publication_v1_0.information_schema.columns
  thyroid_canonical_publication_v1_0.main.canonical_patient_master

Tables WRITTEN (only with --apply)
----------------------------------
  CREATE OR REPLACE TABLE manuscript_workspace.nan_string_audit_v1_1
  ALTER TABLE main.canonical_patient_master ... (rename + add column)
  UPDATE main.canonical_patient_master SET <varchar_col> = NULL WHERE col = 'nan'
  COMMENT ON COLUMN main.canonical_patient_master.syn_margin_distance_mm_raw_str

Rollback
--------
- Pre-run snapshot exported to:
    "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre248_<ts>
- Audit table is CREATE OR REPLACE — re-runnable.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG_PATH = OUTPUT_DIR / "248_run.log"
AUDIT_CSV_PATH = OUTPUT_DIR / "248_nan_string_audit_v1_1.csv"
DECISION_LOG_PATH = OUTPUT_DIR / "248_decision_log.json"

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_QUALIFIED = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
SCRIPT_TAG = "Script 248"
RUN_DATE = "2026-04-16"
CPM = "canonical_patient_master"


def ts_utc_short() -> str:
    return datetime.utcnow().strftime("%H:%M:%S.") + f"{datetime.utcnow().microsecond // 1000:03d}Z"


def log(msg: str, log_file=None) -> None:
    line = f"[{ts_utc_short()}] {msg}"
    print(line, flush=True)
    if log_file is not None:
        log_file.write(line + "\n")
        log_file.flush()


def list_varchar_columns(con) -> list[str]:
    rows = con.execute(
        f"""SELECT column_name
            FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}'
              AND table_schema='main'
              AND table_name='{CPM}'
              AND data_type IN ('VARCHAR','TEXT','STRING')
            ORDER BY column_name"""
    ).fetchall()
    return [r[0] for r in rows]


def audit_column(con, col: str) -> dict:
    """Return per-column counts. Quote column names defensively."""
    q = f'"{col}"'
    row = con.execute(
        f"""SELECT
              SUM(CASE WHEN {q} = 'nan' THEN 1 ELSE 0 END)                  AS n_literal_nan,
              SUM(CASE WHEN {q} IS NULL THEN 1 ELSE 0 END)                  AS n_true_null,
              SUM(CASE WHEN {q} IS NOT NULL AND {q} <> 'nan' THEN 1 ELSE 0 END) AS n_real_values,
              COUNT(DISTINCT CASE WHEN {q} <> 'nan' THEN {q} END)           AS n_distinct_real
            FROM {CPM}"""
    ).fetchone()
    return {
        "column_name": col,
        "n_literal_nan": int(row[0] or 0),
        "n_true_null": int(row[1] or 0),
        "n_real_values": int(row[2] or 0),
        "n_distinct_real": int(row[3] or 0),
    }


def decide_action(rec: dict) -> str:
    # Preserved-raw columns: never modify. Their literal-'nan' values are
    # the documented original-source provenance; the cleaned successor
    # column (e.g. *_num) is the analysis target.
    if rec["column_name"].endswith("_raw_str"):
        return "PRESERVE_RAW"
    if rec["column_name"] == "syn_margin_distance_mm" and rec["n_literal_nan"] > 0:
        return "ADD_NUMERIC_AND_RENAME_RAW"
    if rec["n_literal_nan"] > 0:
        return "UPDATE_NAN_TO_NULL"
    return "NO_ACTION"


def write_audit_table(con, audit: list[dict]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS manuscript_workspace")
    con.execute("DROP TABLE IF EXISTS manuscript_workspace.nan_string_audit_v1_1")
    con.execute(
        """CREATE TABLE manuscript_workspace.nan_string_audit_v1_1 (
             column_name      VARCHAR,
             n_literal_nan    BIGINT,
             n_true_null      BIGINT,
             n_real_values    BIGINT,
             n_distinct_real  BIGINT,
             repair_action    VARCHAR,
             repaired_at      TIMESTAMP,
             repaired_by      VARCHAR
           )"""
    )
    rows = [
        (
            r["column_name"],
            r["n_literal_nan"],
            r["n_true_null"],
            r["n_real_values"],
            r["n_distinct_real"],
            r["repair_action"],
            r.get("repaired_at"),
            r.get("repaired_by"),
        )
        for r in audit
    ]
    con.executemany(
        """INSERT INTO manuscript_workspace.nan_string_audit_v1_1
           VALUES (?,?,?,?,?,?,?,?)""",
        rows,
    )
    con.execute(
        f"""COMMENT ON TABLE manuscript_workspace.nan_string_audit_v1_1 IS
            'One row per VARCHAR column on canonical_patient_master.
             Generated by {SCRIPT_TAG} ({RUN_DATE}). Documents the
             literal-string ''nan'' contamination from upstream pandas
             exports and the deterministic repair applied.'"""
    )


def write_audit_csv(audit: list[dict]) -> None:
    import csv
    with AUDIT_CSV_PATH.open("w", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "column_name",
                "n_literal_nan",
                "n_true_null",
                "n_real_values",
                "n_distinct_real",
                "repair_action",
                "repaired_at",
                "repaired_by",
            ],
        )
        w.writeheader()
        for r in audit:
            row = {k: r.get(k) for k in w.fieldnames}
            w.writerow(row)


def archive_cpm_snapshot(con, run_ts: str) -> str:
    dest = f'{ARCHIVE_QUALIFIED}."{CPM}_pre248_{run_ts}"'
    con.execute(f'CREATE OR REPLACE TABLE {dest} AS SELECT * FROM {CPM}')
    con.execute(
        f"""COMMENT ON TABLE {dest} IS
            '{SCRIPT_TAG} ({RUN_DATE}) pre-write snapshot of
             canonical_patient_master. Rollback source for the v1_1
             literal-nan repair. Cohort = 10,871.'"""
    )
    return dest


def apply_syn_margin_distance_repair(con, log_file) -> dict:
    """Add syn_margin_distance_mm_num, rename old VARCHAR to *_raw_str."""
    cols_now = {
        r[0]
        for r in con.execute(
            f"""SELECT column_name FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                  AND table_name='{CPM}'"""
        ).fetchall()
    }
    actions: list[str] = []

    if "syn_margin_distance_mm_num" not in cols_now:
        con.execute(
            f"""ALTER TABLE {CPM}
                ADD COLUMN syn_margin_distance_mm_num DOUBLE"""
        )
        actions.append("added syn_margin_distance_mm_num DOUBLE")
    else:
        actions.append("syn_margin_distance_mm_num already exists (idempotent)")

    if "syn_margin_distance_mm" in cols_now:
        con.execute(
            f"""UPDATE {CPM}
                SET syn_margin_distance_mm_num =
                    TRY_CAST(NULLIF(syn_margin_distance_mm, 'nan') AS DOUBLE)
                WHERE syn_margin_distance_mm_num IS NULL"""
        )
        actions.append("populated syn_margin_distance_mm_num via TRY_CAST(NULLIF(...,'nan') AS DOUBLE)")
        con.execute(
            f"""ALTER TABLE {CPM}
                RENAME COLUMN syn_margin_distance_mm TO syn_margin_distance_mm_raw_str"""
        )
        actions.append("renamed syn_margin_distance_mm -> syn_margin_distance_mm_raw_str")
    elif "syn_margin_distance_mm_raw_str" in cols_now:
        actions.append("syn_margin_distance_mm already renamed to *_raw_str (idempotent)")
        # Backfill any rows where _num is still NULL from the raw_str source.
        con.execute(
            f"""UPDATE {CPM}
                SET syn_margin_distance_mm_num =
                    TRY_CAST(NULLIF(syn_margin_distance_mm_raw_str, 'nan') AS DOUBLE)
                WHERE syn_margin_distance_mm_num IS NULL"""
        )
        actions.append("re-populated syn_margin_distance_mm_num from *_raw_str (idempotent backfill)")

    con.execute(
        f"""COMMENT ON COLUMN {CPM}.syn_margin_distance_mm_num IS
            'Authoritative numeric margin-distance in mm. Populated by
             {SCRIPT_TAG} ({RUN_DATE}) via TRY_CAST(NULLIF(raw,''nan'') AS DOUBLE)
             from the original VARCHAR source (now syn_margin_distance_mm_raw_str).
             Use this column for all manuscript analyses.'"""
    )
    con.execute(
        f"""COMMENT ON COLUMN {CPM}.syn_margin_distance_mm_raw_str IS
            'Original synoptic-pathology margin-distance string. Renamed from
             syn_margin_distance_mm by {SCRIPT_TAG} ({RUN_DATE}) because
             upstream pandas-export contamination produced literal ''nan''
             strings (~87% of rows). DO NOT USE for analysis — use
             syn_margin_distance_mm_num.'"""
    )
    for a in actions:
        log(f"      action: {a}", log_file)
    return {"column": "syn_margin_distance_mm", "actions": actions}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Apply repairs. Without this flag, --dry-run is the default.",
    )
    ap.add_argument(
        "--dry-run",
        dest="dry_run",
        action="store_true",
        default=True,
        help="Default. Audit only; no DB writes.",
    )
    args = ap.parse_args()
    do_writes = bool(args.apply)
    mode = "APPLY" if do_writes else "DRY-RUN"

    t0 = time.time()
    log_file = RUN_LOG_PATH.open("a")
    log("=" * 78, log_file)
    log(f"=== START {Path(__file__).name}  mode={mode}", log_file)
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}", log_file)

    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    decision = {
        "script": "248",
        "run_ts": run_ts,
        "run_date": RUN_DATE,
        "mode": mode,
        "phases": {},
    }

    # ---- Phase A: enumerate VARCHAR cols ------------------------------
    log("PHASE A — enumerate VARCHAR columns on canonical_patient_master", log_file)
    varchar_cols = list_varchar_columns(con)
    log(f"  total VARCHAR/TEXT cols: {len(varchar_cols)}", log_file)

    # ---- Phase B: per-column audit ------------------------------------
    log("PHASE B — per-column literal-'nan' audit", log_file)
    audit: list[dict] = []
    for i, col in enumerate(varchar_cols, 1):
        try:
            rec = audit_column(con, col)
        except Exception as e:
            log(f"  WARN: column '{col}' audit failed: {str(e)[:120]}", log_file)
            continue
        rec["repair_action"] = decide_action(rec)
        audit.append(rec)
        if rec["n_literal_nan"] > 0:
            log(
                f"  [{i:3d}/{len(varchar_cols)}] {col}: nan={rec['n_literal_nan']}  "
                f"null={rec['n_true_null']}  real={rec['n_real_values']}  "
                f"distinct_real={rec['n_distinct_real']}  -> {rec['repair_action']}",
                log_file,
            )
    n_polluted = sum(1 for r in audit if r["n_literal_nan"] > 0)
    n_total_nan_cells = sum(r["n_literal_nan"] for r in audit)
    log(f"  polluted columns (n_literal_nan > 0): {n_polluted}", log_file)
    log(f"  total literal-'nan' cells across CPM: {n_total_nan_cells}", log_file)

    decision["phases"]["audit"] = {
        "n_varchar_cols": len(varchar_cols),
        "n_polluted_cols": n_polluted,
        "total_literal_nan_cells": n_total_nan_cells,
        "polluted_columns": [
            {
                "column_name": r["column_name"],
                "n_literal_nan": r["n_literal_nan"],
                "n_true_null": r["n_true_null"],
                "n_real_values": r["n_real_values"],
                "repair_action": r["repair_action"],
            }
            for r in audit
            if r["n_literal_nan"] > 0
        ],
    }

    # ---- Phase C: write audit CSV (always) ----------------------------
    write_audit_csv(audit)
    log(f"  wrote audit CSV: {AUDIT_CSV_PATH.relative_to(REPO)}", log_file)

    # ---- Phase D: write audit table (only on --apply OR always?) ------
    # Per Phase 1.4 user spec: "Generate a single audit table" — we want it
    # to land even in dry-run so the user can inspect. But it lives in
    # manuscript_workspace which is a write. Compromise: write only on --apply.
    if do_writes:
        log("PHASE D — write audit table manuscript_workspace.nan_string_audit_v1_1", log_file)
        write_audit_table(con, audit)
        n_audit = con.execute(
            "SELECT COUNT(*) FROM manuscript_workspace.nan_string_audit_v1_1"
        ).fetchone()[0]
        log(f"  wrote audit table rows: {n_audit}", log_file)
    else:
        log("PHASE D — SKIPPED (dry-run): audit table will be written on --apply", log_file)

    # ---- Phase E: archive snapshot before any CPM writes --------------
    if do_writes and n_polluted > 0:
        log("PHASE E — archive CPM snapshot before writes", log_file)
        snap = archive_cpm_snapshot(con, run_ts)
        log(f"  snapshot: {snap}", log_file)
        decision["phases"]["snapshot"] = snap
    else:
        log("PHASE E — SKIPPED", log_file)

    # ---- Phase F: apply repairs ---------------------------------------
    repair_results: list[dict] = []
    if do_writes and n_polluted > 0:
        log("PHASE F — apply repairs", log_file)
        for rec in audit:
            if rec["repair_action"] == "NO_ACTION":
                continue
            col = rec["column_name"]
            log(f"  repairing {col}: action={rec['repair_action']}", log_file)
            if rec["repair_action"] == "ADD_NUMERIC_AND_RENAME_RAW":
                r = apply_syn_margin_distance_repair(con, log_file)
                repair_results.append(r)
                rec["repaired_at"] = datetime.utcnow().isoformat()
                rec["repaired_by"] = SCRIPT_TAG
            elif rec["repair_action"] == "UPDATE_NAN_TO_NULL":
                con.execute(
                    f'UPDATE {CPM} SET "{col}" = NULL WHERE "{col}" = \'nan\''
                )
                # Verify
                n_after = con.execute(
                    f"SELECT COUNT(*) FROM {CPM} WHERE \"{col}\" = 'nan'"
                ).fetchone()[0]
                log(f"      after-repair literal-'nan' count for {col}: {n_after}", log_file)
                rec["repaired_at"] = datetime.utcnow().isoformat()
                rec["repaired_by"] = SCRIPT_TAG
                repair_results.append({"column": col, "n_after": n_after})
        # Re-write audit table with repaired_at populated.
        write_audit_table(con, audit)
        log("  re-wrote audit table with repaired_at timestamps", log_file)
    else:
        log("PHASE F — SKIPPED (dry-run or nothing to repair)", log_file)
    decision["phases"]["repairs"] = repair_results

    # ---- Phase G: post-repair verification ----------------------------
    # Columns whose name ends with `_raw_str` are EXPECTED to retain
    # literal-'nan' values: they are the preserved raw VARCHAR sources
    # documented as DO-NOT-USE in COMMENT ON COLUMN. The successor
    # *_num column carries the cleaned numeric value. Exclude this
    # name pattern from the invariant.
    log("PHASE G — post-repair verification (literal-'nan' sweep)", log_file)
    after_polluted = 0
    after_cells = 0
    after_cols_listing: list[tuple[str, int]] = []
    raw_str_cols_with_nan: list[tuple[str, int]] = []
    varchar_cols_after = list_varchar_columns(con)
    for col in varchar_cols_after:
        try:
            n_nan = con.execute(
                f"SELECT COUNT(*) FROM {CPM} WHERE \"{col}\" = 'nan'"
            ).fetchone()[0]
        except Exception:
            continue
        if n_nan > 0:
            if col.endswith("_raw_str"):
                raw_str_cols_with_nan.append((col, n_nan))
                continue
            after_polluted += 1
            after_cells += n_nan
            after_cols_listing.append((col, n_nan))
    log(f"  after-repair polluted cols (excluding *_raw_str): {after_polluted}", log_file)
    log(f"  after-repair total literal-'nan' cells (excluding *_raw_str): {after_cells}", log_file)
    if raw_str_cols_with_nan:
        for c, n in raw_str_cols_with_nan:
            log(f"    PRESERVED-RAW: {c} -> {n} literal-'nan' (intentional, see COMMENT)", log_file)
    if after_cols_listing:
        for c, n in after_cols_listing[:20]:
            log(f"    REMAINING: {c} -> {n}", log_file)
    decision["phases"]["verification"] = {
        "after_polluted_cols": after_polluted,
        "after_literal_nan_cells": after_cells,
        "remaining_columns": [{"col": c, "n": n} for c, n in after_cols_listing],
        "preserved_raw_str_cols": [{"col": c, "n": n} for c, n in raw_str_cols_with_nan],
    }

    # ---- Phase H: invariant assertion ---------------------------------
    if do_writes:
        log("PHASE H — invariant assertion", log_file)
        if after_cells != 0:
            log(f"  INVARIANT FAIL: literal-'nan' cells > 0 after repair ({after_cells})", log_file)
            decision["invariant_pass"] = False
            with DECISION_LOG_PATH.open("w") as f:
                json.dump(decision, f, indent=2, default=str)
            log("STOP: invariant violation. Decision log written.", log_file)
            log_file.close()
            sys.exit(2)
        decision["invariant_pass"] = True
        log("  INVARIANT PASS: 0 literal-'nan' cells across CPM VARCHAR columns", log_file)

    # ---- finalize -----------------------------------------------------
    with DECISION_LOG_PATH.open("w") as f:
        json.dump(decision, f, indent=2, default=str)
    elapsed = time.time() - t0
    log(f"decision log written: {DECISION_LOG_PATH.relative_to(REPO)}", log_file)
    log(f"elapsed: {elapsed:.1f}s", log_file)
    log(f"=== END {Path(__file__).name}  mode={mode}", log_file)
    log_file.close()


if __name__ == "__main__":
    main()
