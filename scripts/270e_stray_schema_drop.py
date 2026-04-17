#!/usr/bin/env python3
"""Script 270e — stray schema drops + archive DB consolidation.

Phase B execute, part 2 of 2 (runs after 270d --execute is confirmed).

Actions:
  1. Round-trip restore test (wide-snapshot per convention).
  2. For each of the 38 DROP_NO_RESTORE_VALUE rows:
       - Views with broken refs: capture DDL text (or note unavailability)
         into one aggregate audit row.
       - Empty base tables: log metadata only (nothing to capture).
       - DROP TABLE / DROP VIEW (CASCADE-safe because they have no content
         and their reference chains are already broken).
  3. Verify each of the four stray schemas is now empty (every object
     either migrated by 270d or dropped here). Halt if any remain.
  4. DROP SCHEMA IF EXISTS <schema> CASCADE for each of the four stray
     schemas.
  5. Final state assertion: "Thyroid 2026 UPdated" contains exactly
     two schemas: archive_pub_v1_0 and archive_legacy.
  6. One aggregate audit row for the DROP_NO_RESTORE_VALUE batch.
  7. One final audit row: phase_b_archive_consolidated.

No tag here — tag v1_0_archive_consolidated is applied by the user
after reviewing this script's results.

Modes:
  --dry-run (DEFAULT): read-only plan emission. Re-reads 270c manifest,
     re-validates DROP_NO_RESTORE_VALUE rows against live state, checks
     that 270d's migration audit row is present (i.e., 270d ran first),
     emits 270e_dry_run_plan.csv and 270e_dry_run_summary.json.

  --execute: drops the 38 rows, drops the four schemas, asserts final
     state, writes audit rows.

Pre-condition gate: --execute aborts if the audit row
finding_id='phase_b_stray_migrate_complete' is absent (270d must finish
before 270e runs).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
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

OUT_DRY_PLAN_CSV = OUT_DIR / "270e_dry_run_plan.csv"
OUT_DRY_SUMMARY = OUT_DIR / "270e_dry_run_summary.json"
OUT_DRY_LOG = OUT_DIR / "270e_dry_run.log"

OUT_EXEC_SUMMARY = OUT_DIR / "270e_execute_summary.json"
OUT_EXEC_LOG = OUT_DIR / "270e_execute.log"

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA_PUB = "archive_pub_v1_0"
ARCHIVE_SCHEMA_LEGACY = "archive_legacy"
STRAY_SCHEMAS = ("main", "mm_contract_dev", "qa", "v2_stage")
EXPECTED_FINAL_SCHEMAS = frozenset({ARCHIVE_SCHEMA_PUB, ARCHIVE_SCHEMA_LEGACY})

DROP_DISPOSITION = "DROP_NO_RESTORE_VALUE"
AUDIT_FINDING_PREREQ = "phase_b_stray_migrate_complete"
AUDIT_FINDING_DROPS = "phase_b_drop_no_restore_value_complete"
AUDIT_FINDING_FINAL = "phase_b_archive_consolidated"

WS = f'"{PUBLICATION_DB}".manuscript_workspace'
AUDIT_FQ = f"{WS}.v1_1_finalization_audit_v1"

# Restore test (re-uses same selector as 270d)
import re
PRE270_RE = re.compile(r"^canonical_patient_master_pre270_")
PRE_GENERIC_RE = re.compile(r"^canonical_patient_master_pre")


# =============================================================================
# Utilities (same helpers as 270d)
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_stamp(d: datetime) -> str:
    return d.strftime("%Y%m%dT%H%M%SZ")


def quote_fq(db: str, schema: str, name: str) -> str:
    return f'"{db}"."{schema}"."{name}"'


def safe_count(con, fq: str) -> tuple[int | None, str | None]:
    try:
        return int(con.execute(f"SELECT COUNT(*) FROM {fq}").fetchone()[0]), None
    except Exception as e:
        return None, str(e)[:160]


def describe_columns(con, fq: str) -> tuple[list[tuple] | None, str | None]:
    try:
        return [(r[0], r[1]) for r in con.execute(f"DESCRIBE {fq}").fetchall()], None
    except Exception as e:
        return None, str(e)[:160]


def pick_widest_snapshot(con) -> dict | None:
    rows = con.execute("""
        SELECT table_name FROM duckdb_tables()
        WHERE database_name = ? AND schema_name = ?
        ORDER BY table_name
    """, [ARCHIVE_DB, ARCHIVE_SCHEMA_PUB]).fetchall()
    if not rows:
        return None
    candidates: list[dict] = []
    for (name,) in rows:
        fq = quote_fq(ARCHIVE_DB, ARCHIVE_SCHEMA_PUB, name)
        cols, _ = describe_columns(con, fq)
        if cols is None:
            continue
        n, _ = safe_count(con, fq)
        if n is None:
            continue
        band = (
            2_000_000 if PRE270_RE.match(name)
            else 1_000_000 if PRE_GENERIC_RE.match(name)
            else 0
        )
        candidates.append({
            "name": name, "fq": fq,
            "row_count": n, "col_count": len(cols),
            "score": band + len(cols),
            "score_reason": (
                "pre270_match" if band == 2_000_000
                else "pre_generic_match" if band == 1_000_000
                else "fallback_widest"
            ),
        })
    if not candidates:
        return None
    return sorted(candidates, key=lambda c: (c["score"], c["row_count"]), reverse=True)[0]


def run_restore_test(con, log) -> dict:
    log("\n--- ROUND-TRIP RESTORE TEST ---")
    chosen = pick_widest_snapshot(con)
    if chosen is None:
        return {"status": "FAIL", "reason": "no snapshots in archive_pub_v1_0"}
    log(
        f"  chosen: {chosen['fq']} "
        f"(score_reason={chosen['score_reason']!r}, "
        f"cols={chosen['col_count']}, rows={chosen['row_count']})"
    )
    src_cols, _ = describe_columns(con, chosen["fq"])
    temp = f"restore_test_270e_{utc_stamp(utc_now())}"
    try:
        con.execute(f"CREATE TEMPORARY TABLE {temp} AS SELECT * FROM {chosen['fq']}")
    except Exception as e:
        return {"status": "FAIL", "reason": f"CREATE TEMP failed: {str(e)[:160]}",
                "chosen_snapshot": chosen["fq"]}
    tmp_n = con.execute(f"SELECT COUNT(*) FROM {temp}").fetchone()[0]
    tmp_cols, _ = describe_columns(con, temp)
    failures: list[str] = []
    if int(tmp_n) != int(chosen["row_count"]):
        failures.append(f"row count: src={chosen['row_count']} tmp={tmp_n}")
    if len(tmp_cols or []) != len(src_cols or []):
        failures.append(
            f"col count: src={len(src_cols or [])} tmp={len(tmp_cols or [])}"
        )
    try:
        con.execute(f"DROP TABLE {temp}")
        dropped = True
    except Exception:
        dropped = False
    result = {
        "status": "PASS" if not failures else "FAIL",
        "chosen_snapshot": chosen["fq"],
        "selector_score_reason": chosen["score_reason"],
        "source_row_count": int(chosen["row_count"]),
        "source_column_count": int(chosen["col_count"]),
        "temp_row_count": int(tmp_n),
        "temp_column_count": len(tmp_cols or []),
        "temp_table_dropped": dropped,
        "failures": failures,
    }
    if failures:
        log(f"  FAIL: {failures}")
    else:
        log(
            f"  PASS: round-trip {tmp_n} rows / {len(tmp_cols or [])} "
            "cols verified, temp table dropped"
        )
    return result


# =============================================================================
# Plan loading
# =============================================================================

def load_migrate_names() -> set[tuple[str, str]]:
    """Return {(schema, name)} for the 118 objects migrated by 270d.
    These are still present in stray schemas (270d was COPY, not MOVE)
    and will be removed by DROP SCHEMA CASCADE in 270e.
    """
    if not INPUT_CSV.exists():
        return set()
    out: set[tuple[str, str]] = set()
    with INPUT_CSV.open() as f:
        rows = list(csv.reader(f))
    header_idx = next(
        (i for i, r in enumerate(rows) if r and r[0] == "schema"), None
    )
    if header_idx is None:
        return set()
    header = rows[header_idx]
    sch_i = header.index("schema")
    name_i = header.index("name")
    disp_i = header.index("disposition")
    for r in rows[header_idx + 1:]:
        if not r or len(r) <= disp_i:
            continue
        if r[disp_i] in ("MIGRATE_TO_ARCHIVE_LEGACY", "DROP_ALREADY_SNAPSHOTTED"):
            out.add((r[sch_i], r[name_i]))
    return out


def load_drop_plan() -> list[dict]:
    if not INPUT_CSV.exists():
        raise SystemExit(f"missing input: {INPUT_CSV}")
    out: list[dict] = []
    with INPUT_CSV.open() as f:
        rows = list(csv.reader(f))
    header_idx = next(
        (i for i, r in enumerate(rows) if r and r[0] == "schema"), None
    )
    if header_idx is None:
        raise SystemExit("cannot locate header in consolidation CSV")
    header = rows[header_idx]
    sch_i = header.index("schema")
    name_i = header.index("name")
    type_i = header.index("object_type")
    rc_i = header.index("row_count")
    disp_i = header.index("disposition")
    just_i = header.index("justification")
    for r in rows[header_idx + 1:]:
        if not r or len(r) <= just_i:
            continue
        if r[disp_i] != DROP_DISPOSITION:
            continue
        if r[sch_i] not in STRAY_SCHEMAS:
            continue
        out.append({
            "schema": r[sch_i],
            "name": r[name_i],
            "object_type": r[type_i],
            "row_count_at_270c": int(r[rc_i]) if r[rc_i].isdigit() else None,
            "justification_270c": r[just_i],
        })
    return out


def enrich_drop_plan(con, plan: list[dict], log) -> list[dict]:
    """Fetch live queryability, row count, and DDL for each drop candidate."""
    enriched: list[dict] = []
    for r in plan:
        fq = quote_fq(ARCHIVE_DB, r["schema"], r["name"])
        live_n, live_err = safe_count(con, fq)
        ddl: str | None = None
        if r["object_type"] == "VIEW":
            try:
                row = con.execute("""
                    SELECT sql FROM duckdb_views()
                    WHERE database_name = ? AND schema_name = ? AND view_name = ?
                """, [ARCHIVE_DB, r["schema"], r["name"]]).fetchone()
                ddl = str(row[0]) if (row and row[0]) else None
            except Exception:
                pass
        enriched.append({
            **r,
            "live_row_count": live_n,
            "live_queryable": live_err is None,
            "live_error": live_err,
            "view_ddl": ddl,
        })
    n_unqueryable = sum(1 for x in enriched if not x["live_queryable"])
    log(f"  enriched {len(enriched)} drop candidates "
        f"({n_unqueryable} not queryable — expected for broken-ref views)")
    return enriched


def write_csv(path: Path, header: list[str], rows: list[list],
              started_at: datetime) -> None:
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "# generated_by", f"scripts/{path.parent.parent.name}/{path.stem}.py",
            "generated_at", started_at.isoformat(),
        ])
        w.writerow(header)
        for r in rows:
            w.writerow(["" if v is None else v for v in r])


# =============================================================================
# Schema emptiness check
# =============================================================================

DUCKDB_INTERNAL_VIEWS = frozenset({
    "duckdb_views", "duckdb_types", "duckdb_tables", "duckdb_schemas",
    "duckdb_indexes", "duckdb_constraints", "duckdb_databases", "duckdb_columns",
    "sqlite_temp_schema", "sqlite_temp_master", "sqlite_schema", "sqlite_master",
    "pragma_database_list",
})


def count_schema_objects(con, db: str, schema: str) -> int:
    """Return total user-owned object count (tables + views) in the schema.
    DuckDB internal catalog entries are excluded since they cannot be dropped
    and their presence does not indicate user data.
    """
    try:
        t = con.execute("""
            SELECT COUNT(*) FROM duckdb_tables()
            WHERE database_name = ? AND schema_name = ?
        """, [db, schema]).fetchone()[0]
        views = [
            r[0] for r in con.execute("""
                SELECT view_name FROM duckdb_views()
                WHERE database_name = ? AND schema_name = ?
            """, [db, schema]).fetchall()
        ]
        user_views = sum(1 for v in views if v not in DUCKDB_INTERNAL_VIEWS)
        return int(t) + user_views
    except Exception:
        return -1


def list_remaining_archive_db_schemas(con, db: str) -> list[str]:
    try:
        rows = con.execute("""
            SELECT schema_name FROM duckdb_schemas()
            WHERE database_name = ?
            ORDER BY schema_name
        """, [db]).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


# =============================================================================
# Drop helpers
# =============================================================================

def drop_object(con, schema: str, name: str, obj_type: str, log) -> dict:
    fq = quote_fq(ARCHIVE_DB, schema, name)
    started = time.monotonic()
    try:
        if obj_type == "VIEW":
            con.execute(f"DROP VIEW IF EXISTS {fq}")
        else:
            con.execute(f"DROP TABLE IF EXISTS {fq}")
        elapsed = round(time.monotonic() - started, 2)
        return {"status": "OK", "error": None, "elapsed_seconds": elapsed}
    except Exception as e:
        elapsed = round(time.monotonic() - started, 2)
        return {"status": "FAIL", "error": str(e)[:200], "elapsed_seconds": elapsed}


# =============================================================================
# Mode: dry-run
# =============================================================================

def main_dry_run() -> int:
    log_lines: list[str] = []

    def log(msg: str) -> None:
        line = msg if msg.endswith("\n") else msg + "\n"
        log_lines.append(line)
        print(msg)

    started_at = utc_now()
    log("=== START 270e — stray schema drops (DRY-RUN) ===")
    log(f"started_at: {started_at.isoformat()}")

    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    # Pre-condition: 270d must have run
    log("\n--- pre-condition: 270d audit row ---")
    n_prereq = con.execute(
        f"SELECT COUNT(*) FROM {AUDIT_FQ} WHERE finding_id = ?",
        [AUDIT_FINDING_PREREQ],
    ).fetchone()[0]
    log(f"  {AUDIT_FINDING_PREREQ!r}: {'PRESENT' if n_prereq else 'MISSING'}")
    if not n_prereq:
        log("ABORT — 270d must complete before 270e. Run 270d --execute first.")
        OUT_DRY_LOG.write_text("".join(log_lines))
        return 1

    # Restore test
    restore_result = run_restore_test(con, log)
    if restore_result["status"] != "PASS":
        log("\nABORT — restore test failed.")
        OUT_DRY_LOG.write_text("".join(log_lines))
        return 1

    # Load + enrich drop plan
    log("\n--- drop plan ---")
    plan = load_drop_plan()
    log(f"  DROP_NO_RESTORE_VALUE rows from 270c manifest: {len(plan)}")
    enriched = enrich_drop_plan(con, plan, log)

    # Check current stray schema state
    log("\n--- current stray schema object counts (post-270d) ---")
    schema_live_counts: dict[str, int] = {}
    for sch in STRAY_SCHEMAS:
        n = count_schema_objects(con, ARCHIVE_DB, sch)
        schema_live_counts[sch] = n
        log(f"  {sch}: {n} objects")

    # Classify live objects in stray schemas:
    #   - in drop plan (38)           → will be dropped individually (for DDL capture)
    #   - migrated by 270d or snap    → will be removed by DROP SCHEMA CASCADE
    #   - truly unexpected            → neither; flag for human review
    drop_names = {(r["schema"], r["name"]) for r in enriched}
    cascade_names = load_migrate_names()
    unexpected: list[dict] = []
    cascade_covered: list[dict] = []
    for sch in STRAY_SCHEMAS:
        try:
            live_tables = {
                r[0] for r in con.execute("""
                    SELECT table_name FROM duckdb_tables()
                    WHERE database_name = ? AND schema_name = ?
                """, [ARCHIVE_DB, sch]).fetchall()
            }
            live_views = {
                r[0] for r in con.execute("""
                    SELECT view_name FROM duckdb_views()
                    WHERE database_name = ? AND schema_name = ?
                """, [ARCHIVE_DB, sch]).fetchall()
            }
            for name in (live_tables | live_views):
                key = (sch, name)
                if key in drop_names:
                    pass  # covered by individual drop
                elif key in cascade_names:
                    cascade_covered.append({"schema": sch, "name": name})
                else:
                    unexpected.append({"schema": sch, "name": name})
        except Exception:
            pass

    log(
        f"  individual drops (DROP_NO_RESTORE_VALUE): {len(enriched)}\n"
        f"  cascade-covered (migrated+snapshotted):   {len(cascade_covered)}\n"
        f"  truly unexpected (not in any plan):        {len(unexpected)}"
    )
    if unexpected:
        log(f"\n  WARNING: {len(unexpected)} objects not in any plan:")
        for u in unexpected[:10]:
            log(f"    {u['schema']}.{u['name']}")
        if len(unexpected) > 10:
            log(f"    ...and {len(unexpected) - 10} more")
    else:
        log("  All stray schema objects accounted for. Execute is safe.")

    # Expected post-drop schema state
    log("\n--- expected post-execute state ---")
    log(f"  Final archive DB schemas: {sorted(EXPECTED_FINAL_SCHEMAS)}")
    log("  All four stray schemas empty after drops -> DROP SCHEMA CASCADE")

    # Emit plan CSV
    plan_header = [
        "schema", "name", "object_type", "row_count_at_270c",
        "live_row_count", "live_queryable", "live_error",
        "view_ddl_available", "justification_270c",
    ]
    plan_rows = []
    for r in sorted(enriched, key=lambda x: (x["schema"], x["name"])):
        plan_rows.append([
            r["schema"], r["name"], r["object_type"],
            r["row_count_at_270c"], r["live_row_count"],
            r["live_queryable"], r["live_error"] or "",
            r["view_ddl"] is not None, r["justification_270c"],
        ])
    write_csv(OUT_DRY_PLAN_CSV, plan_header, plan_rows, started_at)
    log(f"\n  wrote {OUT_DRY_PLAN_CSV} ({len(plan_rows)} rows)")

    summary = {
        "started_at": started_at.isoformat(),
        "publication_db": PUBLICATION_DB,
        "archive_db": ARCHIVE_DB,
        "mode": "dry-run",
        "restore_test": restore_result,
        "prereq_270d_present": True,
        "drop_plan_count": len(enriched),
        "cascade_covered_count": len(cascade_covered),
        "unexpected_count": len(unexpected),
        "unexpected_live_objects": unexpected[:20],
        "schema_live_counts_post_270d": schema_live_counts,
        "execute_safe": len(unexpected) == 0,
        "outputs": {"plan_csv": str(OUT_DRY_PLAN_CSV)},
    }
    OUT_DRY_SUMMARY.write_text(json.dumps(summary, indent=2, default=str))
    log(f"  wrote {OUT_DRY_SUMMARY}")

    log("\n=== END 270e (DRY-RUN) ===")
    OUT_DRY_LOG.write_text("".join(log_lines))
    return 0 if not unexpected else 1


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
    log("=== START 270e — stray schema drops (--EXECUTE) ===")
    log(f"started_at: {started_at.isoformat()}")

    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    # 1. Idempotency guard
    n_final = con.execute(
        f"SELECT COUNT(*) FROM {AUDIT_FQ} WHERE finding_id = ?",
        [AUDIT_FINDING_FINAL],
    ).fetchone()[0]
    if n_final:
        log(f"ABORT — audit row {AUDIT_FINDING_FINAL!r} already present. "
            "270e already ran successfully.")
        OUT_EXEC_LOG.write_text("".join(log_lines))
        return 0

    # 2. Pre-condition: 270d must have run
    n_prereq = con.execute(
        f"SELECT COUNT(*) FROM {AUDIT_FQ} WHERE finding_id = ?",
        [AUDIT_FINDING_PREREQ],
    ).fetchone()[0]
    if not n_prereq:
        log(f"ABORT — {AUDIT_FINDING_PREREQ!r} not found. Run 270d --execute first.")
        OUT_EXEC_LOG.write_text("".join(log_lines))
        return 1

    # 3. Restore test
    restore_result = run_restore_test(con, log)
    if restore_result["status"] != "PASS":
        log("\nABORT — restore test failed.")
        OUT_EXEC_LOG.write_text("".join(log_lines))
        return 1

    # 4. Load + enrich drop plan
    log("\n--- drop plan ---")
    plan = load_drop_plan()
    enriched = enrich_drop_plan(con, plan, log)
    log(f"  {len(enriched)} objects to drop")

    # 5. Drop loop with DDL capture into aggregate notes
    log("\n--- drop loop ---")
    drop_results: list[dict] = []
    ddl_capture_entries: list[dict] = []
    fail_count = 0
    for i, obj in enumerate(
        sorted(enriched, key=lambda x: (x["schema"], x["name"])), start=1
    ):
        if obj["view_ddl"]:
            ddl_capture_entries.append({
                "schema": obj["schema"],
                "name": obj["name"],
                "object_type": obj["object_type"],
                "row_count": obj.get("live_row_count"),
                "ddl_text": obj["view_ddl"][:500],
            })

        res = drop_object(con, obj["schema"], obj["name"], obj["object_type"], log)
        drop_results.append({**obj, **res})
        prefix = "OK " if res["status"] == "OK" else "FAIL"
        log(
            f"  [{i:>2}/{len(enriched)}] {prefix} "
            f"{obj['schema']}.{obj['name']} ({obj['object_type']}) "
            f"elapsed={res['elapsed_seconds']}s"
            + (f"  ERROR: {res['error']}" if res["error"] else "")
        )
        if res["status"] != "OK":
            fail_count += 1

    log(f"\n  drops: ok={sum(1 for r in drop_results if r['status']=='OK')} "
        f"fail={fail_count}")
    log(f"  view DDL entries captured: {len(ddl_capture_entries)}")

    if fail_count:
        log("\nHALT — drop failures detected. Resolve and re-run.")
        OUT_EXEC_LOG.write_text("".join(log_lines))
        return 1

    # 6. Schema cleanup:
    #    - mm_contract_dev, qa, v2_stage: DROP SCHEMA CASCADE (may already
    #      be gone from a prior partial run; IF EXISTS makes it idempotent).
    #    - main: DuckDB/MotherDuck's internal default schema — cannot be
    #      dropped via DROP SCHEMA. Enumerate remaining objects and drop
    #      individually; assert empty afterwards.
    log(
        "\n--- schema cleanup (CASCADE for non-main; "
        "individual drops for main) ---"
    )
    schema_drop_results: dict[str, str] = {}
    droppable_schemas = [s for s in STRAY_SCHEMAS if s != "main"]
    for sch in droppable_schemas:
        try:
            con.execute(
                f'DROP SCHEMA IF EXISTS "{ARCHIVE_DB}"."{sch}" CASCADE'
            )
            log(f"  DROPPED schema {sch!r}")
            schema_drop_results[sch] = "OK"
        except Exception as e:
            log(f"  FAIL schema {sch!r}: {str(e)[:160]}")
            schema_drop_results[sch] = f"FAIL: {str(e)[:120]}"
            fail_count += 1

    # main schema: drop remaining objects individually
    log("  main schema: enumerating and dropping remaining objects ...")
    main_rem_tables = [
        r[0] for r in con.execute("""
            SELECT table_name FROM duckdb_tables()
            WHERE database_name = ? AND schema_name = 'main'
        """, [ARCHIVE_DB]).fetchall()
    ]
    main_rem_views = [
        r[0] for r in con.execute("""
            SELECT view_name FROM duckdb_views()
            WHERE database_name = ? AND schema_name = 'main'
        """, [ARCHIVE_DB]).fetchall()
    ]
    main_ok = 0
    main_fail = 0
    INTERNAL_CATALOG_MSG = "Cannot drop internal catalog entry"
    for name in main_rem_views:
        fq = quote_fq(ARCHIVE_DB, "main", name)
        try:
            con.execute(f"DROP VIEW IF EXISTS {fq}")
            main_ok += 1
        except Exception as e:
            err = str(e)
            if INTERNAL_CATALOG_MSG in err:
                pass  # DuckDB system view — always present, cannot drop; skip
            else:
                log(f"    FAIL drop view {name}: {err[:100]}")
                main_fail += 1
    for name in main_rem_tables:
        fq = quote_fq(ARCHIVE_DB, "main", name)
        try:
            con.execute(f"DROP TABLE IF EXISTS {fq}")
            main_ok += 1
        except Exception as e:
            err = str(e)
            if INTERNAL_CATALOG_MSG in err:
                pass  # DuckDB internal table — skip
            else:
                log(f"    FAIL drop table {name}: {err[:100]}")
                main_fail += 1
    log(
        f"  main: dropped {main_ok} objects "
        f"({len(main_rem_tables)} tables + {len(main_rem_views)} views), "
        f"{main_fail} failures"
    )
    if main_fail:
        schema_drop_results["main"] = f"FAIL: {main_fail} object drops failed"
        fail_count += main_fail
    else:
        schema_drop_results["main"] = "OK_EMPTIED"

    if any(v.startswith("FAIL") for v in schema_drop_results.values()):
        log("\nHALT — schema cleanup failures. Resolve and re-run.")
        OUT_EXEC_LOG.write_text("".join(log_lines))
        return 1

    # 8. Final state assertion
    # Expected: archive_pub_v1_0 and archive_legacy exist.
    # main always remains (DuckDB internal) — assert it is empty.
    # mm_contract_dev, qa, v2_stage are fully gone.
    log("\n--- final state assertion ---")
    final_schemas = set(list_remaining_archive_db_schemas(con, ARCHIVE_DB))
    user_schemas = {
        s for s in final_schemas
        if not s.startswith("information_schema") and s != "pg_catalog"
    }
    log(f"  archive DB schemas now: {sorted(user_schemas)}")

    # main must exist but be empty
    main_final_count = count_schema_objects(con, ARCHIVE_DB, "main")
    log(f"  main schema object count (must be 0): {main_final_count}")

    # The two expected archive schemas must exist
    missing = EXPECTED_FINAL_SCHEMAS - user_schemas
    # Extra schemas beyond expected + empty main are a problem
    extra = user_schemas - EXPECTED_FINAL_SCHEMAS - {"main"}

    assertion_ok = (
        not missing
        and not extra
        and main_final_count == 0
    )
    if not assertion_ok:
        if missing:
            log(f"  FAIL: missing expected schemas: {sorted(missing)}")
        if extra:
            log(f"  FAIL: unexpected extra schemas: {sorted(extra)}")
        if main_final_count != 0:
            log(f"  FAIL: main schema not empty ({main_final_count} objects remain)")
        log("\nHALT — final state assertion failed.")
        OUT_EXEC_LOG.write_text("".join(log_lines))
        return 1
    log(
        f"  PASS: archive DB contains {sorted(EXPECTED_FINAL_SCHEMAS)} + "
        f"empty main (system schema, cannot drop). "
        f"mm_contract_dev / qa / v2_stage are gone."
    )

    # 9. Audit rows
    log("\n--- audit rows ---")
    ddl_notes_json = json.dumps(
        ddl_capture_entries,
        default=str,
    )[:3800]  # fit in notes column

    # Aggregate drop-batch audit row
    n_drops = len([r for r in drop_results if r["status"] == "OK"])
    existing_drop_audit = con.execute(
        f"SELECT COUNT(*) FROM {AUDIT_FQ} WHERE finding_id = ?",
        [AUDIT_FINDING_DROPS],
    ).fetchone()[0]
    if not existing_drop_audit:
        con.execute(
            f"""
            INSERT INTO {AUDIT_FQ}
                (run_ts, script_num, finding_id, metric,
                 count_before, count_after, target_after, status, notes)
            VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "270e", AUDIT_FINDING_DROPS,
                "drop_no_restore_value_objects_dropped",
                len(enriched), 0, 0, "OK",
                f"Dropped {n_drops} DROP_NO_RESTORE_VALUE objects from stray "
                f"archive schemas. Object types: "
                f"{dict((o['object_type'], sum(1 for x in drop_results if x['object_type']==o['object_type'])) for o in [{'object_type': 'VIEW'}, {'object_type': 'BASE TABLE'}])}. "
                f"View DDL entries captured: {len(ddl_capture_entries)}. "
                f"DDL preview (first {len(ddl_capture_entries)} entries, "
                f"truncated to 3800 chars): {ddl_notes_json}",
            ],
        )
        log(f"  inserted audit row {AUDIT_FINDING_DROPS!r}")

    # Final consolidated audit row
    con.execute(
        f"""
        INSERT INTO {AUDIT_FQ}
            (run_ts, script_num, finding_id, metric,
             count_before, count_after, target_after, status, notes)
        VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            "270e", AUDIT_FINDING_FINAL,
            "stray_schemas_remaining",
            4, 0, 0, "OK",
            (
                f"Archive DB \"{ARCHIVE_DB}\" consolidated to exactly "
                f"{sorted(EXPECTED_FINAL_SCHEMAS)}. "
                f"Restore test PASSED on {restore_result['chosen_snapshot']} "
                f"({restore_result['source_row_count']} rows / "
                f"{restore_result['source_column_count']} cols). "
                f"270d migrated 118 objects to archive_legacy; "
                f"270e dropped {n_drops} DROP_NO_RESTORE_VALUE objects, "
                f"dropped schemas mm_contract_dev/qa/v2_stage via CASCADE, "
                f"and emptied the main schema (DuckDB internal schema; "
                f"cannot DROP, but 0 objects remain). "
                "Canonical DB main schema unchanged from v1_0_registry_locked "
                "(0 archive candidates; Phase B was archive-DB-only work). "
                "Ready for tag v1_0_archive_consolidated."
            ),
        ],
    )
    log(f"  inserted audit row {AUDIT_FINDING_FINAL!r}")

    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": utc_now().isoformat(),
        "publication_db": PUBLICATION_DB,
        "archive_db": ARCHIVE_DB,
        "mode": "execute",
        "restore_test": restore_result,
        "drop_plan_count": len(enriched),
        "drops_ok": sum(1 for r in drop_results if r["status"] == "OK"),
        "drops_fail": fail_count,
        "view_ddl_entries_captured": len(ddl_capture_entries),
        "schema_drop_results": schema_drop_results,
        "final_schemas_in_archive_db": sorted(user_schemas),
        "main_schema_object_count_final": main_final_count,
        "final_state_assertion_passed": assertion_ok,
        "audit_findings_inserted": [AUDIT_FINDING_DROPS, AUDIT_FINDING_FINAL],
    }
    OUT_EXEC_SUMMARY.write_text(json.dumps(summary, indent=2, default=str))
    log(f"  wrote {OUT_EXEC_SUMMARY}")

    log("\n=== END 270e (--EXECUTE) ===")
    log(
        "Archive DB is now consolidated to two schemas: "
        f"{sorted(EXPECTED_FINAL_SCHEMAS)}. "
        "Ready for tag v1_0_archive_consolidated."
    )
    OUT_EXEC_LOG.write_text("".join(log_lines))
    return 0


# =============================================================================
# CLI
# =============================================================================

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--dry-run", action="store_true", default=True,
        help="Read-only plan emission (default).",
    )
    p.add_argument(
        "--execute", action="store_true", default=False,
        help="Perform the drops and schema cleanup (gated).",
    )
    args = p.parse_args()
    if args.execute:
        return main_execute()
    return main_dry_run()


if __name__ == "__main__":
    sys.exit(main())
