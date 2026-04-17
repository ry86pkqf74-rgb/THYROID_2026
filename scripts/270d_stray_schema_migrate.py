#!/usr/bin/env python3
"""Script 270d — stray schema migration (MIGRATE_TO_ARCHIVE_LEGACY only).

Phase B execute, part 1 of 2. Migrates the 243 MIGRATE_TO_ARCHIVE_LEGACY
rows from the four stray archive-DB schemas (main, mm_contract_dev, qa,
v2_stage) into "Thyroid 2026 UPdated".archive_legacy under names like

    archive_legacy.<source_schema>__<source_name>_<UTC>

Does NOT touch the 39 DROP_NO_RESTORE_VALUE rows (Script 270e handles
those) and does NOT drop the stray schemas themselves (270e). The
intermediate state — stray schemas exist but every object in them has
a snapshot under archive_legacy — is intentional. A reviewer recovering
from a halt mid-270d sees a clean partial-migration state, not a
half-archived database.

Modes:

  --dry-run (DEFAULT)
      Read-only. Re-validates input from
      scripts/output/270c_stray_schema_consolidation.csv against live
      state, picks the wide-schema restore-test target per
      restore_test_prefers_widest_schema convention, runs the round-
      trip restore test, runs budget pre-flight, emits
      270d_dry_run_plan.csv + 270d_dry_run_summary.json.

  --execute
      Performs the migrations. Per-object 30-second timeout (DuckDB
      statement_timeout + wall-clock backstop). Captures view DDL for
      the migrated views into archive_legacy.__view_ddl_preservation_v1
      and materializes them as
      <name>_<UTC>_view_materialized. Writes one aggregate audit row
      to v1_1_finalization_audit_v1.

Idempotency:

  --execute aborts if archive_legacy already contains any object whose
  name matches the prefix "main__" / "mm_contract_dev__" / "qa__" /
  "v2_stage__" with today's UTC date. Re-running on a different day
  with state already migrated is also blocked: aborts if the audit row
  finding_id='phase_b_stray_migrate_complete' exists.

Tag: do NOT tag here. Tag after 270e (v1_0_archive_consolidated).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

INPUT_CSV = OUT_DIR / "270c_stray_schema_consolidation.csv"

OUT_DRY_PLAN_CSV = OUT_DIR / "270d_dry_run_plan.csv"
OUT_DRY_SUMMARY = OUT_DIR / "270d_dry_run_summary.json"
OUT_DRY_LOG = OUT_DIR / "270d_dry_run.log"
OUT_DRY_RESTORE = OUT_DIR / "270d_dry_run_restore_test.json"

OUT_EXEC_RESULTS_CSV = OUT_DIR / "270d_execute_results.csv"
OUT_EXEC_SUMMARY = OUT_DIR / "270d_execute_summary.json"
OUT_EXEC_LOG = OUT_DIR / "270d_execute.log"
OUT_EXEC_RESTORE = OUT_DIR / "270d_execute_restore_test.json"

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA_PUB = "archive_pub_v1_0"
ARCHIVE_SCHEMA_LEGACY = "archive_legacy"
STRAY_SCHEMAS = ("main", "mm_contract_dev", "qa", "v2_stage")
TARGET_DISPOSITION = "MIGRATE_TO_ARCHIVE_LEGACY"

VIEW_DDL_TABLE = "__view_ddl_preservation_v1"

WS = f'"{PUBLICATION_DB}".manuscript_workspace'
AUDIT_FQ = f"{WS}.v1_1_finalization_audit_v1"

BUDGET_MAX_MIGRATIONS = 250
BUDGET_PER_OBJECT_TIMEOUT_SECONDS = 30
BUDGET_PER_OBJECT_TIMEOUT_MS = BUDGET_PER_OBJECT_TIMEOUT_SECONDS * 1000

AUDIT_FINDING_ID = "phase_b_stray_migrate_complete"

# Regex for restore-test snapshot scoring
PRE270_RE = re.compile(r"^canonical_patient_master_pre270_")
PRE_GENERIC_RE = re.compile(r"^canonical_patient_master_pre")


# =============================================================================
# Utilities
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_stamp(d: datetime) -> str:
    return d.strftime("%Y%m%dT%H%M%SZ")


def quote_fq(db: str, schema: str, name: str) -> str:
    return f'"{db}"."{schema}"."{name}"'


def safe_count(con, fq_table: str) -> tuple[int | None, str | None]:
    try:
        n = con.execute(f"SELECT COUNT(*) FROM {fq_table}").fetchone()[0]
        return int(n), None
    except Exception as e:
        return None, str(e)[:160]


def describe_columns(con, fq: str) -> tuple[list[tuple[str, str]] | None, str | None]:
    try:
        rows = con.execute(f"DESCRIBE {fq}").fetchall()
        return [(r[0], r[1]) for r in rows], None
    except Exception as e:
        return None, str(e)[:160]


# =============================================================================
# Restore test (wide-snapshot selector — per convention
# restore_test_prefers_widest_schema)
# =============================================================================

def pick_widest_snapshot(con) -> dict | None:
    """Return {schema, name, fq, row_count, col_count, score, score_reason}
    or None if no candidates exist.

    Selection ladder per restore_test_prefers_widest_schema convention:
      1. widest column count among canonical_patient_master_pre270_* candidates
      2. widest column count among canonical_patient_master_pre*  candidates
      3. widest column count among ALL archive_pub_v1_0 base tables
      Ties broken by row_count DESC, then table_name DESC for stability.
    """
    rows = con.execute(f"""
        SELECT table_name FROM duckdb_tables()
        WHERE database_name = ?
          AND schema_name   = ?
        ORDER BY table_name
    """, [ARCHIVE_DB, ARCHIVE_SCHEMA_PUB]).fetchall()
    if not rows:
        return None

    candidates: list[dict] = []
    for (name,) in rows:
        fq = quote_fq(ARCHIVE_DB, ARCHIVE_SCHEMA_PUB, name)
        cols, derr = describe_columns(con, fq)
        if cols is None:
            continue
        n, cerr = safe_count(con, fq)
        if cerr is not None:
            continue
        candidates.append({
            "schema": ARCHIVE_SCHEMA_PUB,
            "name": name,
            "fq": fq,
            "row_count": n or 0,
            "col_count": len(cols),
        })
    if not candidates:
        return None

    def score(c: dict) -> tuple[int, int, int, str]:
        # Higher is better (sorted DESC). Ladder via large additive bands.
        band = 0
        reason = "fallback_widest_in_archive_pub"
        if PRE270_RE.match(c["name"]):
            band = 2_000_000
            reason = "pre270_match"
        elif PRE_GENERIC_RE.match(c["name"]):
            band = 1_000_000
            reason = "pre_generic_match"
        c["score_reason"] = reason
        return (band + c["col_count"], c["row_count"], 0, c["name"])

    chosen = sorted(candidates, key=score, reverse=True)[0]
    chosen["score"] = score(chosen)[0]
    return chosen


def run_restore_test(con, log) -> dict:
    """Run the round-trip restore test with the widest available snapshot."""
    log("\n--- ROUND-TRIP RESTORE TEST (wide-snapshot selector) ---")
    chosen = pick_widest_snapshot(con)
    if chosen is None:
        msg = "no candidates in archive_pub_v1_0 — cannot run restore test"
        log(f"  FAIL: {msg}")
        return {"status": "FAIL", "reason": msg}

    log(
        f"  chosen: {chosen['fq']} "
        f"(score_reason={chosen.get('score_reason')!r}, "
        f"cols={chosen['col_count']}, rows={chosen['row_count']})"
    )
    src_cols, _ = describe_columns(con, chosen["fq"])
    src_n = chosen["row_count"]

    temp_name = f"restore_test_270d_{utc_stamp(utc_now())}"
    try:
        con.execute(
            f"CREATE TEMPORARY TABLE {temp_name} AS SELECT * FROM {chosen['fq']}"
        )
    except Exception as e:
        msg = f"CREATE TEMPORARY TABLE failed: {str(e)[:200]}"
        log(f"  FAIL: {msg}")
        return {
            "status": "FAIL",
            "reason": msg,
            "chosen_snapshot": chosen["fq"],
            "source_row_count": int(src_n),
            "source_column_count": len(src_cols or []),
        }

    tmp_n = con.execute(f"SELECT COUNT(*) FROM {temp_name}").fetchone()[0]
    tmp_cols, _ = describe_columns(con, temp_name)

    failures: list[str] = []
    if int(tmp_n) != int(src_n):
        failures.append(f"row count mismatch: src={src_n} tmp={tmp_n}")
    if len(tmp_cols or []) != len(src_cols or []):
        failures.append(
            f"column count mismatch: src={len(src_cols or [])} "
            f"tmp={len(tmp_cols or [])}"
        )
    src_map = {c[0]: c[1] for c in (src_cols or [])}
    tmp_map = {c[0]: c[1] for c in (tmp_cols or [])}
    type_diffs: list[str] = []
    for col, src_type in src_map.items():
        if col not in tmp_map:
            type_diffs.append(f"missing in tmp: {col}")
            continue
        if tmp_map[col] != src_type:
            type_diffs.append(f"{col}: src={src_type} tmp={tmp_map[col]}")
    if type_diffs:
        failures.append(
            f"column type diffs: {type_diffs[:5]}"
            + (" ..." if len(type_diffs) > 5 else "")
        )

    try:
        con.execute(f"DROP TABLE {temp_name}")
        dropped = True
    except Exception as e:
        dropped = False
        failures.append(f"drop temp failed: {str(e)[:120]}")

    result = {
        "status": "PASS" if not failures else "FAIL",
        "chosen_snapshot": chosen["fq"],
        "selector_score_reason": chosen.get("score_reason"),
        "source_row_count": int(src_n),
        "source_column_count": len(src_cols or []),
        "temp_row_count": int(tmp_n),
        "temp_column_count": len(tmp_cols or []),
        "type_diffs": type_diffs,
        "temp_table_dropped": dropped,
        "failures": failures,
    }
    if failures:
        log(f"  FAIL: {failures}")
    else:
        log(
            f"  PASS: round-trip {tmp_n} rows / {len(tmp_cols or [])} "
            f"cols verified, temp table dropped"
        )
    return result


# =============================================================================
# Plan loading & live re-validation
# =============================================================================

def load_migrate_plan() -> list[dict]:
    """Read 270c consolidation CSV; return MIGRATE_TO_ARCHIVE_LEGACY rows."""
    if not INPUT_CSV.exists():
        raise SystemExit(f"missing input plan: {INPUT_CSV}")
    out: list[dict] = []
    with INPUT_CSV.open() as f:
        rows = list(csv.reader(f))
    header_idx = next(
        (i for i, r in enumerate(rows) if r and r[0] == "schema"), None
    )
    if header_idx is None:
        raise SystemExit(f"could not locate header in {INPUT_CSV}")
    header = rows[header_idx]
    sch_i = header.index("schema")
    name_i = header.index("name")
    type_i = header.index("object_type")
    rc_i = header.index("row_count")
    disp_i = header.index("disposition")
    proposed_i = header.index("proposed_target_name")
    just_i = header.index("justification")
    for r in rows[header_idx + 1:]:
        if not r or len(r) <= proposed_i:
            continue
        if r[disp_i] != TARGET_DISPOSITION:
            continue
        if r[sch_i] not in STRAY_SCHEMAS:
            continue
        out.append({
            "schema": r[sch_i],
            "name": r[name_i],
            "object_type": r[type_i],
            "row_count_at_270c": int(r[rc_i]) if r[rc_i].isdigit() else None,
            "proposed_target_name_270c": r[proposed_i],
            "justification_270c": r[just_i],
        })
    return out


def count_divergent_rows() -> tuple[int, list[dict]]:
    """Read 270c consolidation CSV; return (count, list) of DIVERGENT rows.

    270d refuses to --execute while any DIVERGENT rows exist. They must
    be resolved (re-run 270c with truth, or human-classify each row to
    DROP_ALREADY_SNAPSHOTTED / MIGRATE_TO_ARCHIVE_LEGACY) first.
    """
    if not INPUT_CSV.exists():
        return 0, []
    out: list[dict] = []
    with INPUT_CSV.open() as f:
        rows = list(csv.reader(f))
    header_idx = next(
        (i for i, r in enumerate(rows) if r and r[0] == "schema"), None
    )
    if header_idx is None:
        return 0, []
    header = rows[header_idx]
    sch_i = header.index("schema")
    name_i = header.index("name")
    rc_i = header.index("row_count")
    disp_i = header.index("disposition")
    just_i = header.index("justification")
    for r in rows[header_idx + 1:]:
        if not r or len(r) <= just_i:
            continue
        if r[disp_i] != "DIVERGENT":
            continue
        out.append({
            "schema": r[sch_i],
            "name": r[name_i],
            "row_count": r[rc_i],
            "justification": r[just_i],
        })
    return len(out), out


def revalidate_against_live(con, plan: list[dict], log) -> list[dict]:
    """For each plan row, re-fetch live row_count, queryability, and
    object_type. Flag drift. Recompute proposed_target_name with the
    270d-run UTC stamp (the 270c stamp is stale)."""
    run_ts = utc_stamp(utc_now())
    enriched: list[dict] = []
    drift_count = 0
    for r in plan:
        fq = quote_fq(ARCHIVE_DB, r["schema"], r["name"])
        live_n, live_err = safe_count(con, fq)
        cols_info, _ = describe_columns(con, fq)
        live_cols = len(cols_info) if cols_info else None
        drift_notes: list[str] = []
        if live_err is not None:
            drift_notes.append(f"NOT_QUERYABLE_NOW: {live_err[:80]}")
            drift_count += 1
        elif r["row_count_at_270c"] is not None and live_n != r["row_count_at_270c"]:
            drift_notes.append(
                f"ROW_COUNT_DRIFT: 270c={r['row_count_at_270c']} live={live_n}"
            )
            drift_count += 1
        new_target = f"{r['schema']}__{r['name']}_{run_ts}"
        enriched.append({
            **r,
            "live_row_count": live_n,
            "live_column_count": live_cols,
            "live_queryable": live_err is None,
            "live_error": live_err,
            "drift_notes": ";".join(drift_notes) if drift_notes else "",
            "proposed_target_name_270d": new_target,
        })
    log(f"  re-validated {len(enriched)} plan rows ({drift_count} with drift)")
    return enriched


# =============================================================================
# Execute helpers
# =============================================================================

def ensure_archive_legacy_schema(con, log) -> None:
    con.execute(f'CREATE SCHEMA IF NOT EXISTS "{ARCHIVE_DB}".{ARCHIVE_SCHEMA_LEGACY}')
    log(f"  ensured schema \"{ARCHIVE_DB}\".{ARCHIVE_SCHEMA_LEGACY}")


def ensure_view_ddl_table(con, log) -> None:
    fq = quote_fq(ARCHIVE_DB, ARCHIVE_SCHEMA_LEGACY, VIEW_DDL_TABLE)
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS {fq} (
            source_schema       VARCHAR NOT NULL,
            source_name         VARCHAR NOT NULL,
            view_ddl_text       VARCHAR,
            row_count_at_migrate BIGINT,
            column_count_at_migrate INTEGER,
            materialized_target VARCHAR,
            migrated_at         TIMESTAMP NOT NULL,
            migration_script    VARCHAR NOT NULL
        )
    """)
    log(f"  ensured view DDL preservation table {fq}")


def fetch_view_ddl(con, schema: str, name: str) -> str | None:
    try:
        row = con.execute(f"""
            SELECT sql FROM duckdb_views()
            WHERE database_name = ? AND schema_name = ? AND view_name = ?
        """, [ARCHIVE_DB, schema, name]).fetchone()
        if row and row[0]:
            return str(row[0])
        return None
    except Exception:
        return None


def set_statement_timeout(con, timeout_ms: int, log) -> bool:
    """Try to enforce a per-statement timeout. Returns True on success."""
    try:
        con.execute(f"SET statement_timeout = '{timeout_ms}ms'")
        return True
    except Exception as e:
        log(
            f"  WARN: could not set statement_timeout "
            f"({str(e)[:100]}); using wall-clock backstop only"
        )
        return False


def pre_execute_idempotency_guard(con, run_ts: str, log) -> list[str]:
    """Return list of abort reasons (empty -> safe)."""
    reasons: list[str] = []

    # 1. audit row already present
    n = con.execute(
        f"SELECT COUNT(*) FROM {AUDIT_FQ} WHERE finding_id = ?",
        [AUDIT_FINDING_ID],
    ).fetchone()[0]
    if n:
        reasons.append(
            f"audit row {AUDIT_FINDING_ID!r} already present "
            "(prior 270d --execute completed)"
        )

    # 2. archive_legacy contains migrated objects with prefix matching
    #    today's UTC date (idempotent guard against same-day re-run)
    today_prefix = utc_stamp(utc_now())[:8]  # YYYYMMDD
    try:
        rows = con.execute(f"""
            SELECT table_name FROM duckdb_tables()
            WHERE database_name = ?
              AND schema_name   = ?
              AND (
                table_name LIKE 'main__%' OR
                table_name LIKE 'mm_contract_dev__%' OR
                table_name LIKE 'qa__%' OR
                table_name LIKE 'v2_stage__%'
              )
            UNION ALL
            SELECT view_name FROM duckdb_views()
            WHERE database_name = ?
              AND schema_name   = ?
              AND (
                view_name LIKE 'main__%' OR
                view_name LIKE 'mm_contract_dev__%' OR
                view_name LIKE 'qa__%' OR
                view_name LIKE 'v2_stage__%'
              )
        """, [ARCHIVE_DB, ARCHIVE_SCHEMA_LEGACY, ARCHIVE_DB, ARCHIVE_SCHEMA_LEGACY]).fetchall()
        same_day = [
            r[0] for r in rows
            if today_prefix in r[0]
        ]
        if same_day:
            reasons.append(
                f"{len(same_day)} archive_legacy objects already migrated "
                f"today ({today_prefix}); re-run blocked"
            )
    except Exception as e:
        log(f"  warn: idempotency probe failed (will proceed): {str(e)[:100]}")

    return reasons


def migrate_one_object(
    con,
    obj: dict,
    log,
    use_statement_timeout: bool,
) -> dict:
    """Migrate one stray object. Returns result dict."""
    src_schema = obj["schema"]
    src_name = obj["name"]
    obj_type = obj["object_type"]
    src_fq = quote_fq(ARCHIVE_DB, src_schema, src_name)

    target_name = obj["proposed_target_name_270d"]
    target_fq = quote_fq(ARCHIVE_DB, ARCHIVE_SCHEMA_LEGACY, target_name)

    started = time.monotonic()

    result = {
        "schema": src_schema,
        "name": src_name,
        "object_type": obj_type,
        "source_row_count": obj.get("live_row_count"),
        "source_column_count": obj.get("live_column_count"),
        "target_name": target_name,
        "target_fq": target_fq,
        "view_ddl_preserved": False,
        "view_materialized_target": None,
        "elapsed_seconds": None,
        "status": None,
        "error": None,
    }

    if obj_type == "VIEW":
        # 1. Capture DDL into __view_ddl_preservation_v1
        ddl = fetch_view_ddl(con, src_schema, src_name)
        materialized_target = f"{target_name}_view_materialized"
        materialized_fq = quote_fq(
            ARCHIVE_DB, ARCHIVE_SCHEMA_LEGACY, materialized_target
        )
        try:
            con.execute(
                f"""
                INSERT INTO {quote_fq(ARCHIVE_DB, ARCHIVE_SCHEMA_LEGACY, VIEW_DDL_TABLE)}
                    (source_schema, source_name, view_ddl_text,
                     row_count_at_migrate, column_count_at_migrate,
                     materialized_target, migrated_at, migration_script)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                """,
                [
                    src_schema, src_name, ddl,
                    obj.get("live_row_count"),
                    obj.get("live_column_count"),
                    materialized_target, "270d_stray_schema_migrate",
                ],
            )
            result["view_ddl_preserved"] = True
        except Exception as e:
            result["status"] = "FAIL"
            result["error"] = f"view DDL capture failed: {str(e)[:160]}"
            result["elapsed_seconds"] = round(time.monotonic() - started, 2)
            return result

        # 2. Materialize view as table under archive_legacy
        try:
            con.execute(
                f"CREATE TABLE {materialized_fq} AS SELECT * FROM {src_fq}"
            )
            result["view_materialized_target"] = materialized_fq
        except Exception as e:
            result["status"] = "FAIL"
            result["error"] = f"view materialize failed: {str(e)[:160]}"
            result["elapsed_seconds"] = round(time.monotonic() - started, 2)
            return result

        elapsed = time.monotonic() - started
        result["elapsed_seconds"] = round(elapsed, 2)
        if elapsed > BUDGET_PER_OBJECT_TIMEOUT_SECONDS:
            result["status"] = "FAIL"
            result["error"] = (
                f"wall-clock budget exceeded "
                f"({elapsed:.1f}s > {BUDGET_PER_OBJECT_TIMEOUT_SECONDS}s)"
            )
            return result

        result["status"] = "OK"
        return result

    # BASE TABLE path
    try:
        con.execute(
            f"CREATE TABLE {target_fq} AS SELECT * FROM {src_fq}"
        )
    except Exception as e:
        result["status"] = "FAIL"
        result["error"] = f"CTAS failed: {str(e)[:160]}"
        result["elapsed_seconds"] = round(time.monotonic() - started, 2)
        return result

    # Verify row count round-trip
    tgt_n, tgt_err = safe_count(con, target_fq)
    if tgt_err is not None:
        result["status"] = "FAIL"
        result["error"] = f"target verify failed: {tgt_err[:120]}"
        result["elapsed_seconds"] = round(time.monotonic() - started, 2)
        return result

    src_n_now, _ = safe_count(con, src_fq)
    if src_n_now is not None and tgt_n != src_n_now:
        result["status"] = "FAIL"
        result["error"] = (
            f"row count drift: src={src_n_now} target={tgt_n}"
        )
        result["elapsed_seconds"] = round(time.monotonic() - started, 2)
        return result

    elapsed = time.monotonic() - started
    result["elapsed_seconds"] = round(elapsed, 2)
    if elapsed > BUDGET_PER_OBJECT_TIMEOUT_SECONDS:
        result["status"] = "FAIL"
        result["error"] = (
            f"wall-clock budget exceeded "
            f"({elapsed:.1f}s > {BUDGET_PER_OBJECT_TIMEOUT_SECONDS}s); "
            "object migrated but flagged for review"
        )
        return result

    result["status"] = "OK"
    return result


# =============================================================================
# Mode: dry-run
# =============================================================================

def write_csv(path: Path, header: list[str], rows: list[list], started_at: datetime) -> None:
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "# generated_by", "scripts/270d_stray_schema_migrate.py",
            "generated_at", started_at.isoformat(),
        ])
        w.writerow(header)
        for r in rows:
            w.writerow(["" if v is None else v for v in r])


def main_dry_run() -> int:
    log_lines: list[str] = []

    def log(msg: str) -> None:
        line = msg if msg.endswith("\n") else msg + "\n"
        log_lines.append(line)
        print(msg)

    started_at = utc_now()
    log("=== START 270d — stray schema migration (DRY-RUN) ===")
    log(f"started_at: {started_at.isoformat()}")
    log(f"input plan: {INPUT_CSV}")

    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    # 1. Restore test (wide-snapshot per convention)
    restore_result = run_restore_test(con, log)
    OUT_DRY_RESTORE.write_text(json.dumps(restore_result, indent=2, default=str))
    log(f"  wrote {OUT_DRY_RESTORE}")
    if restore_result["status"] != "PASS":
        log("\nABORT — restore test failed; refusing to plan execute.")
        OUT_DRY_LOG.write_text("".join(log_lines))
        return 1

    # 2. Load + revalidate
    plan = load_migrate_plan()
    n_divergent, divergent_rows = count_divergent_rows()
    log(f"\n--- load plan ---")
    log(f"  MIGRATE_TO_ARCHIVE_LEGACY rows in 270c manifest: {len(plan)}")
    log(f"  DIVERGENT rows in 270c manifest: {n_divergent}")
    if n_divergent:
        log("  DIVERGENT rows (270d --execute will refuse while any exist):")
        for r in divergent_rows:
            log(
                f"    - {r['schema']}.{r['name']} (row_count={r['row_count']}): "
                f"{r['justification'][:140]}"
            )
    schema_counts: dict[str, int] = {}
    for r in plan:
        schema_counts[r["schema"]] = schema_counts.get(r["schema"], 0) + 1
    log(f"  per-schema breakdown: {schema_counts}")

    log("\n--- revalidate against live state ---")
    enriched = revalidate_against_live(con, plan, log)

    # 3. Budget pre-flight
    log("\n--- budget pre-flight ---")
    n_total = len(enriched)
    n_drift = sum(1 for r in enriched if r["drift_notes"])
    n_unqueryable = sum(1 for r in enriched if not r["live_queryable"])
    total_rows = sum((r["live_row_count"] or 0) for r in enriched)
    biggest = sorted(enriched, key=lambda x: -(x["live_row_count"] or 0))[:5]
    log(f"  migrate_count: {n_total} (limit {BUDGET_MAX_MIGRATIONS})")
    log(f"  rows_with_drift: {n_drift}")
    log(f"  rows_not_queryable: {n_unqueryable}")
    log(f"  total_rows_to_migrate: {total_rows:,}")
    log(f"  per_object_timeout: {BUDGET_PER_OBJECT_TIMEOUT_SECONDS}s")
    log("  largest 5 by row_count:")
    for r in biggest:
        log(f"    {r['schema']}.{r['name']:<55} rows={r['live_row_count']}")

    halt_reasons: list[str] = []
    if n_total > BUDGET_MAX_MIGRATIONS:
        halt_reasons.append(
            f"migrate_count {n_total} exceeds budget {BUDGET_MAX_MIGRATIONS}"
        )
    if n_unqueryable:
        halt_reasons.append(
            f"{n_unqueryable} rows not queryable in live state — must "
            "re-run 270c or remove them from the plan"
        )
    if n_divergent:
        halt_reasons.append(
            f"{n_divergent} DIVERGENT row(s) in 270c manifest — resolve "
            "(re-run 270c with truth, or human-classify each row to "
            "DROP_ALREADY_SNAPSHOTTED / MIGRATE_TO_ARCHIVE_LEGACY) before "
            "--execute. See dry_run_summary.json for the row list."
        )

    # 4. Emit plan CSV
    plan_header = [
        "schema", "name", "object_type",
        "row_count_at_270c", "live_row_count", "live_column_count",
        "live_queryable", "drift_notes",
        "proposed_target_name_270d", "proposed_target_fq",
    ]
    plan_rows = []
    for r in sorted(enriched, key=lambda x: (x["schema"], x["name"])):
        target_fq = quote_fq(
            ARCHIVE_DB, ARCHIVE_SCHEMA_LEGACY,
            r["proposed_target_name_270d"],
        )
        plan_rows.append([
            r["schema"], r["name"], r["object_type"],
            r["row_count_at_270c"], r["live_row_count"],
            r["live_column_count"], r["live_queryable"],
            r["drift_notes"],
            r["proposed_target_name_270d"], target_fq,
        ])
    write_csv(OUT_DRY_PLAN_CSV, plan_header, plan_rows, started_at)
    log(f"\n  wrote {OUT_DRY_PLAN_CSV} ({len(plan_rows)} rows)")

    # 5. Summary JSON
    summary = {
        "started_at": started_at.isoformat(),
        "publication_db": PUBLICATION_DB,
        "archive_db": ARCHIVE_DB,
        "mode": "dry-run",
        "input_csv": str(INPUT_CSV),
        "restore_test": restore_result,
        "migrate_count": n_total,
        "divergent_count": n_divergent,
        "divergent_rows": divergent_rows,
        "per_schema_counts": schema_counts,
        "rows_with_drift": n_drift,
        "rows_not_queryable": n_unqueryable,
        "total_rows_to_migrate": int(total_rows),
        "budgets": {
            "migrate_count_limit": BUDGET_MAX_MIGRATIONS,
            "per_object_timeout_seconds": BUDGET_PER_OBJECT_TIMEOUT_SECONDS,
        },
        "halt_reasons": halt_reasons,
        "execute_safe": len(halt_reasons) == 0,
        "outputs": {
            "plan_csv": str(OUT_DRY_PLAN_CSV),
            "restore_test_json": str(OUT_DRY_RESTORE),
        },
        "next_action": (
            "Resolve halt_reasons before --execute"
            if halt_reasons else
            "Plan is execute-safe. Re-run with --execute after Logan "
            "reviews 270c_stray_main_review_list.csv (156 rows in main) "
            "and 270c_phase_b_disposition_manifest.csv "
            "(33 KEEP_PENDING_V1_1_DECISION rows)."
        ),
    }
    OUT_DRY_SUMMARY.write_text(json.dumps(summary, indent=2, default=str))
    log(f"  wrote {OUT_DRY_SUMMARY}")

    if halt_reasons:
        log("\n--- halt reasons ---")
        for h in halt_reasons:
            log(f"  - {h}")
        log("Resolve halt reasons before --execute.")

    log(f"\n=== END 270d (DRY-RUN) ===")
    OUT_DRY_LOG.write_text("".join(log_lines))
    return 0 if not halt_reasons else 1


# =============================================================================
# Mode: execute
# =============================================================================

def main_execute() -> int:
    log_lines: list[str] = []

    def log(msg: str) -> None:
        line = msg if msg.endswith("\n") else msg + "\n"
        log_lines.append(line)
        print(msg)

    started_at = utc_now()
    log("=== START 270d — stray schema migration (--EXECUTE) ===")
    log(f"started_at: {started_at.isoformat()}")

    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    # 1. Idempotency guard
    log("\n--- idempotency guard ---")
    abort_reasons = pre_execute_idempotency_guard(con, utc_stamp(started_at), log)
    if abort_reasons:
        log("ABORT:")
        for r in abort_reasons:
            log(f"  - {r}")
        OUT_EXEC_LOG.write_text("".join(log_lines))
        return 0

    # 2. Restore test (must pass)
    restore_result = run_restore_test(con, log)
    OUT_EXEC_RESTORE.write_text(json.dumps(restore_result, indent=2, default=str))
    log(f"  wrote {OUT_EXEC_RESTORE}")
    if restore_result["status"] != "PASS":
        log("\nABORT — restore test failed; refusing to migrate.")
        OUT_EXEC_LOG.write_text("".join(log_lines))
        return 1

    # 3. Re-load + re-validate plan against live state
    plan = load_migrate_plan()
    n_divergent, divergent_rows = count_divergent_rows()
    if n_divergent:
        log(f"\nABORT — {n_divergent} DIVERGENT row(s) in 270c manifest:")
        for r in divergent_rows:
            log(
                f"  - {r['schema']}.{r['name']} (rc={r['row_count']}): "
                f"{r['justification'][:140]}"
            )
        log(
            "Resolve each DIVERGENT row (re-run 270c with truth, or "
            "human-classify to DROP_ALREADY_SNAPSHOTTED / "
            "MIGRATE_TO_ARCHIVE_LEGACY) before --execute."
        )
        OUT_EXEC_LOG.write_text("".join(log_lines))
        return 1

    enriched = revalidate_against_live(con, plan, log)
    n_total = len(enriched)
    log(f"  plan size: {n_total}")

    if n_total > BUDGET_MAX_MIGRATIONS:
        log(
            f"\nABORT — plan size {n_total} exceeds "
            f"budget {BUDGET_MAX_MIGRATIONS}"
        )
        OUT_EXEC_LOG.write_text("".join(log_lines))
        return 1

    n_unqueryable = sum(1 for r in enriched if not r["live_queryable"])
    if n_unqueryable:
        log(
            f"\nABORT — {n_unqueryable} planned rows are not queryable in "
            "live state; re-run 270c to refresh the manifest."
        )
        OUT_EXEC_LOG.write_text("".join(log_lines))
        return 1

    # 4. Schema setup
    log("\n--- schema setup ---")
    ensure_archive_legacy_schema(con, log)
    ensure_view_ddl_table(con, log)
    use_st_timeout = set_statement_timeout(
        con, BUDGET_PER_OBJECT_TIMEOUT_MS, log
    )
    log(f"  statement_timeout enforced: {use_st_timeout}")

    # 5. Migrate loop
    log("\n--- migrate loop ---")
    results: list[dict] = []
    fail_count = 0
    for i, obj in enumerate(
        sorted(enriched, key=lambda x: (x["schema"], x["name"])), start=1
    ):
        res = migrate_one_object(con, obj, log, use_st_timeout)
        results.append(res)
        prefix = "OK " if res["status"] == "OK" else "FAIL"
        log(
            f"  [{i:>3}/{n_total}] {prefix} {res['schema']}.{res['name']:<55} "
            f"-> {res['target_name']}  "
            f"rows={res['source_row_count']} elapsed={res['elapsed_seconds']}s"
            + (f"  ERROR: {res['error']}" if res['error'] else "")
        )
        if res["status"] != "OK":
            fail_count += 1
            # Halt-on-first-failure: leaves a clean partial state with
            # an explicit failure record in execute_results.csv.
            log(
                "\nHALTING on first failure; partial migration recorded in "
                "execute_results.csv. Resolve and re-run."
            )
            break

    # 6. Emit results CSV
    results_header = [
        "schema", "name", "object_type", "source_row_count",
        "source_column_count", "target_name", "target_fq",
        "view_ddl_preserved", "view_materialized_target",
        "elapsed_seconds", "status", "error",
    ]
    res_rows = []
    for r in results:
        res_rows.append([
            r["schema"], r["name"], r["object_type"],
            r["source_row_count"], r["source_column_count"],
            r["target_name"], r["target_fq"],
            r["view_ddl_preserved"], r["view_materialized_target"],
            r["elapsed_seconds"], r["status"], r["error"] or "",
        ])
    write_csv(OUT_EXEC_RESULTS_CSV, results_header, res_rows, started_at)
    log(f"\n  wrote {OUT_EXEC_RESULTS_CSV} ({len(res_rows)} rows)")

    # 7. Aggregate counts per schema, per status
    by_schema_status: dict[tuple[str, str], int] = {}
    for r in results:
        key = (r["schema"], r["status"])
        by_schema_status[key] = by_schema_status.get(key, 0) + 1
    log(f"  by (schema, status): {dict(by_schema_status)}")

    # 8. Audit row (only on full success, otherwise leave the partial-state
    #    audit row for the next attempt to resolve)
    audit_inserted = False
    if fail_count == 0 and len(results) == n_total:
        notes = (
            f"Migrated {n_total} stray-schema objects to "
            f"\"{ARCHIVE_DB}\".{ARCHIVE_SCHEMA_LEGACY}. "
            f"per-schema: {dict(by_schema_status)}. "
            f"Restore test PASSED on {restore_result['chosen_snapshot']} "
            f"({restore_result['source_row_count']} rows / "
            f"{restore_result['source_column_count']} cols). "
            "View DDL preserved in __view_ddl_preservation_v1. "
            "Stray schemas still exist (270e drops them). No tag yet."
        )
        con.execute(
            f"""
            INSERT INTO {AUDIT_FQ}
                (run_ts, script_num, finding_id, metric,
                 count_before, count_after, target_after, status, notes)
            VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ["270d", AUDIT_FINDING_ID,
             "stray_schema_objects_migrated",
             n_total, n_total, n_total, "OK", notes],
        )
        audit_inserted = True
        log(f"\n  inserted audit row finding_id={AUDIT_FINDING_ID!r}")

    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": utc_now().isoformat(),
        "publication_db": PUBLICATION_DB,
        "archive_db": ARCHIVE_DB,
        "mode": "execute",
        "restore_test": restore_result,
        "plan_size": n_total,
        "attempted": len(results),
        "ok_count": sum(1 for r in results if r["status"] == "OK"),
        "fail_count": fail_count,
        "by_schema_status": {
            f"{k[0]}/{k[1]}": v for k, v in by_schema_status.items()
        },
        "view_ddl_rows_preserved": sum(
            1 for r in results
            if r["object_type"] == "VIEW" and r["view_ddl_preserved"]
        ),
        "audit_row_inserted": audit_inserted,
        "outputs": {
            "results_csv": str(OUT_EXEC_RESULTS_CSV),
            "restore_test_json": str(OUT_EXEC_RESTORE),
        },
    }
    OUT_EXEC_SUMMARY.write_text(json.dumps(summary, indent=2, default=str))
    log(f"  wrote {OUT_EXEC_SUMMARY}")

    log(f"\n=== END 270d (--EXECUTE) ===")
    OUT_EXEC_LOG.write_text("".join(log_lines))
    return 0 if fail_count == 0 else 1


# =============================================================================
# CLI
# =============================================================================

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--dry-run", action="store_true", default=True,
        help="Read-only: emit migration plan + summary (default).",
    )
    p.add_argument(
        "--execute", action="store_true", default=False,
        help="Perform the migrations (gated; idempotent guard applies).",
    )
    args = p.parse_args()
    if args.execute:
        return main_execute()
    return main_dry_run()


if __name__ == "__main__":
    sys.exit(main())
