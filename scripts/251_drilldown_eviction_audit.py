#!/usr/bin/env python3
"""
Script 251 — Phase 4 of v1_1 cleanup: drill-down table consistency + eviction audit

Date:    2026-04-16
Author:  THYROID_2026 canonical-finalization run (v1_1 cleanup)
Branch:  cleanup/canonical-finalization-20260416

Purpose
=======
1. Snapshot every main BASE TABLE to "Thyroid 2026 UPdated".archive_pub_v1_0
   with a single pre251 timestamp.
2. Guard sweep for raw_/md_ prefix tables in canonical (audit said zero;
   defensive). If any found: COPY to "Thyroid 2026 UPdated".main and DROP from
   canonical.
3. Content-duplicate detection: for each pair of base tables with identical
   schemas, compute MD5(STRING_AGG(...)) of natural-key cols + non-pk hashes
   to spot strict subset/duplicate. Rename duplicates to DEPRECATED__<name>
   with COMMENT (do NOT drop).
4. Re-verify canonical_patient_master invariants.
5. Verify all 65 manuscript_workspace views resolve.
6. Refresh main.__readme + main.data_dictionary_v240 from live
   information_schema state.

Default mode is --dry-run. Pass --apply to execute writes.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG_PATH = OUTPUT_DIR / "251_run.log"
DECISION_LOG_PATH = OUTPUT_DIR / "251_decision_log.json"

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_QUALIFIED = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
SCRIPT_TAG = "Script 251"
RUN_DATE = "2026-04-16"
CPM = "canonical_patient_master"


def ts_utc_short() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.") + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"


def log(msg: str, log_file=None) -> None:
    line = f"[{ts_utc_short()}] {msg}"
    print(line, flush=True)
    if log_file is not None:
        log_file.write(line + "\n")
        log_file.flush()


# ---------------------------------------------------------------------------
# Phase 4A — list base tables
# ---------------------------------------------------------------------------

def list_base_tables(con) -> list[str]:
    return [
        r[0] for r in con.execute(
            f"""SELECT table_name FROM information_schema.tables
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                  AND table_type='BASE TABLE'
                ORDER BY table_name"""
        ).fetchall()
    ]


# ---------------------------------------------------------------------------
# Phase 4B — snapshot all base tables
# ---------------------------------------------------------------------------

def snapshot_all_tables(con, run_ts: str, tables: list[str], log_file) -> dict:
    snapshots: list[dict] = []
    failures: list[dict] = []
    for i, t in enumerate(tables, 1):
        dest = f"{t}_pre251_{run_ts}"
        full = f'{ARCHIVE_QUALIFIED}."{dest}"'
        try:
            con.execute(f'CREATE OR REPLACE TABLE {full} AS SELECT * FROM main."{t}"')
            n = con.execute(f"SELECT COUNT(*) FROM {full}").fetchone()[0]
            con.execute(
                f"""COMMENT ON TABLE {full} IS
                    '{SCRIPT_TAG} ({RUN_DATE}) snapshot of main.{t} taken
                     before any v1_1 Phase 4 mutations.'"""
            )
            snapshots.append({"source": t, "dest": dest, "rows": n})
            if i % 25 == 0 or i == len(tables):
                log(f"    snapshotted {i}/{len(tables)} tables...", log_file)
        except Exception as e:
            failures.append({"source": t, "error": str(e)[:200]})
            log(f"    FAIL snapshot {t}: {str(e)[:160]}", log_file)
    return {"n_snapshots": len(snapshots), "n_failures": len(failures),
            "snapshots": snapshots, "failures": failures}


# ---------------------------------------------------------------------------
# Phase 4C — raw_/md_ guard sweep
# ---------------------------------------------------------------------------

def raw_md_guard_sweep(con, tables: list[str], do_writes: bool, log_file) -> dict:
    suspect = [t for t in tables if t.startswith("raw_") or t.startswith("md_")]
    actions: list[dict] = []
    if not suspect:
        log("  raw_/md_ guard: 0 suspect tables in canonical (clean)", log_file)
        return {"n_suspect": 0, "actions": []}
    log(f"  raw_/md_ guard: {len(suspect)} suspect tables found", log_file)
    for t in suspect:
        log(f"    suspect: {t}", log_file)
        if do_writes:
            target = f'"{ARCHIVE_DB}".main."{t}_evicted_from_canonical"'
            con.execute(f'CREATE OR REPLACE TABLE {target} AS SELECT * FROM main."{t}"')
            con.execute(
                f"""COMMENT ON TABLE {target} IS
                    '{SCRIPT_TAG} ({RUN_DATE}) evicted from canonical DB
                     (raw_/md_ prefix violates canonical naming convention).'"""
            )
            con.execute(f'DROP TABLE main."{t}"')
            actions.append({"table": t, "action": "MOVED + DROPPED", "target": target})
        else:
            actions.append({"table": t, "action": "WOULD MOVE + DROP"})
    return {"n_suspect": len(suspect), "actions": actions}


# ---------------------------------------------------------------------------
# Phase 4D — content-duplicate detection
# ---------------------------------------------------------------------------

def get_table_signature(con, t: str) -> tuple[tuple[str, ...], int]:
    """Return (sorted col-name tuple, row count) for a table."""
    cols = tuple(sorted(
        r[0] for r in con.execute(
            f"""SELECT column_name FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                  AND table_name='{t}'"""
        ).fetchall()
    ))
    n = con.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
    return cols, n


def content_hash(con, t: str, cols: tuple[str, ...]) -> str:
    """MD5 over concatenated column values; expensive for large tables."""
    if not cols:
        return ""
    cast_list = ", ".join(f'COALESCE(CAST("{c}" AS VARCHAR), \'\\\\N\')' for c in cols[:10])
    try:
        h = con.execute(
            f"""SELECT MD5(STRING_AGG(CONCAT_WS('|', {cast_list}), '\\n'
                ORDER BY CONCAT_WS('|', {cast_list})))
                FROM main."{t}" """
        ).fetchone()[0]
    except Exception as e:
        return f"ERR:{str(e)[:80]}"
    return h or ""


def find_content_duplicates(con, tables: list[str], log_file) -> list[dict]:
    """Group tables by (sorted-col-tuple, row count); within each group, hash content."""
    sigs: dict[tuple[tuple[str, ...], int], list[str]] = {}
    for t in tables:
        try:
            cols, n = get_table_signature(con, t)
        except Exception:
            continue
        sigs.setdefault((cols, n), []).append(t)
    candidates = [
        (cols, n, ts)
        for (cols, n), ts in sigs.items()
        if len(ts) > 1 and n > 0 and n < 100_000  # skip empty + huge
    ]
    log(f"  candidate duplicate groups (matching schema + row count): {len(candidates)}", log_file)
    dupes: list[dict] = []
    for cols, n, ts in candidates:
        log(f"    candidate group ({len(ts)} tables, {n} rows): {ts}", log_file)
        hashes: dict[str, str] = {}
        for t in ts:
            hashes[t] = content_hash(con, t, cols)
        # Group by hash
        by_hash: dict[str, list[str]] = {}
        for t, h in hashes.items():
            by_hash.setdefault(h, []).append(t)
        for h, group in by_hash.items():
            if len(group) > 1 and not h.startswith("ERR"):
                dupes.append({
                    "tables": sorted(group),
                    "row_count": n,
                    "n_cols": len(cols),
                    "hash_preview": h[:32],
                })
                log(f"      DUPLICATE: {group} (hash={h[:16]}...)", log_file)
    return dupes


def rename_duplicates(con, dupes: list[dict], do_writes: bool, log_file) -> list[dict]:
    """Rename all-but-the-first table in each duplicate group to DEPRECATED__."""
    actions: list[dict] = []
    for d in dupes:
        canonical = sorted(d["tables"])[0]
        for t in d["tables"]:
            if t == canonical:
                continue
            new_name = f"DEPRECATED__{t}"
            log(f"    rename {t} -> {new_name} (canonical kept: {canonical})", log_file)
            if do_writes:
                con.execute(f'ALTER TABLE main."{t}" RENAME TO "{new_name}"')
                con.execute(
                    f"""COMMENT ON TABLE main."{new_name}" IS
                        '{SCRIPT_TAG} ({RUN_DATE}). Content-duplicate of
                         main.{canonical} (identical schema + row count + MD5
                         content hash). Renamed to DEPRECATED__ prefix.
                         Do not query; use {canonical} instead.'"""
                )
            actions.append({
                "from": t, "to": new_name, "kept": canonical,
                "applied": do_writes,
            })
    return actions


# ---------------------------------------------------------------------------
# Phase 4E — invariant + view check
# ---------------------------------------------------------------------------

def cohort_invariants(con) -> dict:
    n = con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0]
    nd = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {CPM}").fetchone()[0]
    nnr = con.execute(f"SELECT COUNT(*) FROM {CPM} WHERE research_id IS NULL").fetchone()[0]
    nnf = con.execute(f"SELECT COUNT(*) FROM {CPM} WHERE fna_path_outcome IS NULL").fetchone()[0]
    return {"n_rows": n, "n_distinct_rid": nd, "n_null_rid": nnr, "n_null_fpo": nnf}


def view_compile_sweep(con) -> dict:
    views = con.execute(
        """SELECT table_schema, table_name FROM information_schema.views
           WHERE table_catalog = ?
             AND table_schema IN ('main','manuscript_workspace')
           ORDER BY table_schema, table_name""",
        [PUBLICATION_DB],
    ).fetchall()
    broken: list[dict] = []
    counts: list[dict] = []
    for sch, name in views:
        try:
            n = con.execute(f'SELECT COUNT(*) FROM "{sch}"."{name}"').fetchone()[0]
            counts.append({"view": f"{sch}.{name}", "n_rows": n})
        except Exception as e:
            broken.append({"view": f"{sch}.{name}", "error": str(e)[:160]})
    return {
        "total": len(views),
        "passed": len(counts),
        "broken": len(broken),
        "broken_list": broken,
        "ws_views_with_zero_rows": [c for c in counts if c["n_rows"] == 0],
    }


# ---------------------------------------------------------------------------
# Phase 4F — refresh __readme + data_dictionary_v240
# ---------------------------------------------------------------------------

def refresh_readme(con, do_writes: bool, log_file) -> dict:
    """Re-derive __readme rows from live information_schema. Preserve descriptions."""
    existing = {
        r[0]: r[2]
        for r in con.execute("SELECT table_name, rows, description FROM main.__readme").fetchall()
    }
    candidates = con.execute(
        f"""SELECT table_name FROM information_schema.tables
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_type='BASE TABLE'
            ORDER BY table_name"""
    ).fetchall()
    new_rows = []
    for (t,) in candidates:
        try:
            n = con.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
        except Exception:
            n = 0
        desc = existing.get(t) or "TODO: describe"
        new_rows.append((t, n, desc))
    log(f"  __readme: {len(new_rows)} BASE TABLE rows", log_file)
    if not do_writes:
        log(f"  WOULD rebuild __readme with {len(new_rows)} rows", log_file)
        return {"n_rows": len(new_rows), "applied": False}
    con.execute("DROP TABLE IF EXISTS main.__readme")
    con.execute(
        "CREATE TABLE main.__readme (table_name VARCHAR, rows BIGINT, description VARCHAR)"
    )
    con.executemany("INSERT INTO main.__readme VALUES (?, ?, ?)", new_rows)
    con.execute(
        f"""COMMENT ON TABLE main.__readme IS
            'Refreshed by {SCRIPT_TAG} ({RUN_DATE}) from queryable enumeration.
             One row per main BASE TABLE; row counts match live state.'"""
    )
    return {"n_rows": len(new_rows), "applied": True}


def refresh_data_dictionary(con, do_writes: bool, log_file) -> dict:
    """Refresh data_dictionary_v240 to reflect current CPM column state.

    Preserve all existing description / status / replacement_column_name fields
    where present; add rows for new CPM columns and surface drops.
    """
    if not do_writes:
        cpm_now = {
            r[0]
            for r in con.execute(
                f"""SELECT column_name FROM information_schema.columns
                    WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                      AND table_name='{CPM}'"""
            ).fetchall()
        }
        dict_now = {
            r[0]
            for r in con.execute(
                "SELECT column_name FROM main.data_dictionary_v240"
            ).fetchall()
        }
        in_cpm_not_dict = sorted(cpm_now - dict_now)
        in_dict_not_cpm = sorted(dict_now - cpm_now)
        log(f"  data_dictionary_v240 vs CPM: in_cpm_not_dict={len(in_cpm_not_dict)} in_dict_not_cpm={len(in_dict_not_cpm)}", log_file)
        return {
            "applied": False,
            "in_cpm_not_dict": in_cpm_not_dict,
            "in_dict_not_cpm": in_dict_not_cpm,
        }
    # Apply: bring data_dictionary_v240 in sync with live CPM cols (preserve descriptions).
    cpm_cols = con.execute(
        f"""SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='{CPM}'
            ORDER BY ordinal_position"""
    ).fetchall()
    dict_now = {
        r[0]: r
        for r in con.execute(
            """SELECT * FROM main.data_dictionary_v240"""
        ).fetchall()
    }
    cpm_now_set = {c for c, _, _ in cpm_cols}
    # Removed-from-CPM rows: keep the row but mark status='removed'
    removed = [c for c in dict_now if c not in cpm_now_set]
    # New CPM cols not in dict: insert with placeholder
    added = [c for c, _, _ in cpm_cols if c not in dict_now]
    # Update strategy: surgical inserts/updates (do not full-rebuild to preserve
    # description text we don't want to lose).
    n_inserted = 0
    cols_descriptor = con.execute(
        """SELECT column_name FROM information_schema.columns
           WHERE table_catalog=? AND table_schema='main'
             AND table_name='data_dictionary_v240'
           ORDER BY ordinal_position""",
        [PUBLICATION_DB],
    ).fetchall()
    dict_col_names = [r[0] for r in cols_descriptor]
    log(f"  data_dictionary_v240 schema: {dict_col_names}", log_file)
    for new_col in added:
        # Find data_type + ordinal
        dt = next((dt for c, dt, op in cpm_cols if c == new_col), "VARCHAR")
        op = next((op for c, dt, op in cpm_cols if c == new_col), 0)
        # Build positional insert; default everything else to NULL
        insert_vals = []
        for k in dict_col_names:
            if k == "column_name":
                insert_vals.append(new_col)
            elif k == "data_type":
                insert_vals.append(dt)
            elif k == "ordinal_position":
                insert_vals.append(op)
            elif k == "description":
                insert_vals.append(f"AUTO-ADDED by {SCRIPT_TAG} ({RUN_DATE}); needs description")
            else:
                insert_vals.append(None)
        placeholders = ", ".join("?" * len(dict_col_names))
        col_ids = ", ".join(f'"{c}"' for c in dict_col_names)
        con.execute(
            f"INSERT INTO main.data_dictionary_v240 ({col_ids}) VALUES ({placeholders})",
            insert_vals,
        )
        n_inserted += 1
    # Mark removed rows with status='removed' (if status col exists)
    n_marked = 0
    if "status" in dict_col_names:
        for c in removed:
            con.execute(
                f"""UPDATE main.data_dictionary_v240
                    SET status='removed'
                    WHERE column_name='{c.replace("'", "''")}'"""
            )
            n_marked += 1
    log(f"  data_dictionary_v240: +{n_inserted} new rows, {n_marked} marked removed", log_file)
    return {
        "applied": True,
        "n_inserted": n_inserted,
        "n_marked_removed": n_marked,
        "added_cols": added,
        "removed_cols": removed,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Execute writes. Without this flag, --dry-run is the default.")
    ap.add_argument("--dry-run", dest="dry_run", action="store_true", default=True,
                    help="Default. Audit + plan only; no DB writes.")
    ap.add_argument("--skip-snapshot", action="store_true",
                    help="Skip Phase 4B (full table snapshots). Useful for re-runs.")
    args = ap.parse_args()
    do_writes = bool(args.apply)
    mode = "APPLY" if do_writes else "DRY-RUN"

    t0 = time.time()
    log_file = RUN_LOG_PATH.open("a")
    log("=" * 78, log_file)
    log(f"=== START {Path(__file__).name}  mode={mode}", log_file)
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}", log_file)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    decision: dict = {
        "script": "251", "run_ts": run_ts, "run_date": RUN_DATE, "mode": mode,
        "phases": {},
    }

    # ---- 4A enumerate -----------------------------------------------
    log("PHASE 4A — enumerate main BASE TABLEs", log_file)
    tables = list_base_tables(con)
    log(f"  base tables: {len(tables)}", log_file)
    decision["phases"]["enumerate"] = {"n_tables": len(tables), "tables": tables}

    # ---- 4B snapshots -----------------------------------------------
    if args.skip_snapshot:
        log("PHASE 4B — SKIPPED via --skip-snapshot", log_file)
    elif do_writes:
        log("PHASE 4B — snapshot every base table to archive_pub_v1_0", log_file)
        snap = snapshot_all_tables(con, run_ts, tables, log_file)
        decision["phases"]["snapshots"] = {
            "n_snapshots": snap["n_snapshots"],
            "n_failures": snap["n_failures"],
            "failures": snap["failures"],
        }
    else:
        log("PHASE 4B — SKIPPED (dry-run; use --apply to execute snapshots)", log_file)

    # ---- 4C raw_/md_ guard ------------------------------------------
    log("PHASE 4C — raw_/md_ guard sweep", log_file)
    raw = raw_md_guard_sweep(con, tables, do_writes, log_file)
    decision["phases"]["raw_md_guard"] = raw

    # ---- 4D content-duplicate detection -----------------------------
    log("PHASE 4D — content-duplicate detection", log_file)
    dupes = find_content_duplicates(con, tables, log_file)
    decision["phases"]["dupes_detected"] = dupes
    rename_actions = rename_duplicates(con, dupes, do_writes, log_file)
    decision["phases"]["dup_renames"] = rename_actions

    # ---- 4E invariants + view sweep ---------------------------------
    log("PHASE 4E — re-verify invariants + 65 manuscript_workspace views", log_file)
    inv = cohort_invariants(con)
    log(f"  CPM: rows={inv['n_rows']} distinct_rid={inv['n_distinct_rid']} "
        f"null_rid={inv['n_null_rid']} null_fpo={inv['n_null_fpo']}", log_file)
    decision["phases"]["invariants"] = inv
    if inv["n_rows"] != 10871:
        raise RuntimeError(f"CPM rows {inv['n_rows']} != 10871")
    if inv["n_distinct_rid"] != 10871:
        raise RuntimeError(f"distinct rid {inv['n_distinct_rid']} != 10871")
    if inv["n_null_rid"] != 0 or inv["n_null_fpo"] != 0:
        raise RuntimeError("NULL rid or fpo > 0")

    sweep = view_compile_sweep(con)
    log(f"  views: {sweep['passed']}/{sweep['total']} pass, {sweep['broken']} broken", log_file)
    if sweep["broken"]:
        for v in sweep["broken_list"][:5]:
            log(f"    BROKEN: {v['view']} -> {v['error']}", log_file)
    decision["phases"]["view_sweep"] = sweep
    if sweep["broken"] != 0:
        raise RuntimeError(f"{sweep['broken']} broken views — must be 0")

    # ---- 4F refresh __readme + data_dictionary_v240 -----------------
    log("PHASE 4F — refresh __readme", log_file)
    rd = refresh_readme(con, do_writes, log_file)
    decision["phases"]["readme_refresh"] = rd

    log("PHASE 4F — refresh data_dictionary_v240", log_file)
    dd = refresh_data_dictionary(con, do_writes, log_file)
    decision["phases"]["data_dict_refresh"] = dd

    # Final summary
    log(f"=== END elapsed={time.time()-t0:.1f}s", log_file)
    with DECISION_LOG_PATH.open("w") as f:
        json.dump(decision, f, indent=2, default=str)
    log(f"decision log written: {DECISION_LOG_PATH.relative_to(REPO)}", log_file)
    log_file.close()


if __name__ == "__main__":
    main()
