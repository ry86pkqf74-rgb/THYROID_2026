#!/usr/bin/env python3
"""
Script 258 — Re-point canonical_detail_pointer_v1 + verify registry

Re-runs Script 250's pointer rebuild logic (via subprocess --apply) so that
`manuscript_workspace.canonical_detail_pointer_v1` reflects the post-Script-257
publication-DB state. Then runs strict assertions:

  - 0 detail_table_registry_v1 rows where feeds_master_columns IS NULL or
    contains 'TODO' or '(unset)'
  - canonical_detail_pointer_v1 resolves every (non-self) detail_table_name
    to an EXISTING table in the publication DB
  - the three v1_1 priority drill-downs are present in the pointer mapping:
      canonical_us_nodule_characteristics_v1   (TIRADS per-nodule-per-exam)
      canonical_tumor_characteristics_v1       (per-resected-tumor)
      thyroglobulin_lab_canonical_v1           (Tg / TgAb)

Also takes its own pre258 snapshot of the pointer view DDL into archive
for an independent rollback footprint.

Default --dry-run; pass --apply.
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import (  # noqa: E402
    ARCHIVE_QUALIFIED, ensure_archive_schema, ensure_audit_table,
    make_logger, record_audit, utc_ts, write_decision_log,
)

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
RUN_LOG = OUTPUT_DIR / "258_run.log"
DECISION_LOG = OUTPUT_DIR / "258_decision_log.json"
SCRIPT_TAG = "Script 258"
SCRIPT_NUM = "258"
RUN_DATE = "2026-04-16"

REGISTRY = "manuscript_workspace.detail_table_registry_v1"
POINTER = "manuscript_workspace.canonical_detail_pointer_v1"

PRIORITY_DRILLDOWNS = [
    "canonical_us_nodule_characteristics_v1",
    "canonical_tumor_characteristics_v1",
    "thyroglobulin_lab_canonical_v1",
]


def snapshot_pointer_ddl(con, run_ts: str, log) -> str:
    ensure_archive_schema(con)
    dest = f"canonical_detail_pointer_v1_pre258_{run_ts}"
    full = f'{ARCHIVE_QUALIFIED}."{dest}"'
    con.execute(f"DROP TABLE IF EXISTS {full}")
    con.execute(f"""
        CREATE TABLE {full} (
            view_name VARCHAR,
            view_definition VARCHAR,
            snapshotted_at TIMESTAMP
        )
    """)
    defn = con.execute(f"""
        SELECT view_definition FROM information_schema.views
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
          AND table_name='canonical_detail_pointer_v1'
    """).fetchone()
    con.execute(
        f"INSERT INTO {full} VALUES (?, ?, current_timestamp)",
        ["canonical_detail_pointer_v1", defn[0] if defn else None],
    )
    con.execute(
        f"COMMENT ON TABLE {full} IS '{SCRIPT_TAG} ({RUN_DATE}) pre-rebuild "
        f"DDL snapshot of canonical_detail_pointer_v1.'"
    )
    log(f"  archived pointer DDL: {full}")
    return full


def call_script_250_apply(log) -> int:
    """Invoke scripts/250_registry_pointer_rebuild.py --apply as a subprocess."""
    cmd = [sys.executable, str(REPO / "scripts" / "250_registry_pointer_rebuild.py"),
           "--apply"]
    log(f"  invoking: {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(REPO), capture_output=True, text=True)
    # Echo last 12 lines of stdout for visibility
    tail = proc.stdout.splitlines()[-12:]
    for ln in tail:
        log(f"    [250] {ln}")
    if proc.returncode != 0:
        log(f"  Script 250 FAILED rc={proc.returncode}")
        log(f"  stderr tail: {proc.stderr[-400:]}")
    return proc.returncode


def assert_registry_clean(con, log) -> dict:
    n_null = int(con.execute(f"""
        SELECT COUNT(*) FROM {REGISTRY}
        WHERE feeds_master_columns IS NULL
           OR TRIM(COALESCE(feeds_master_columns,'')) = ''
    """).fetchone()[0])
    n_todo = int(con.execute(f"""
        SELECT COUNT(*) FROM {REGISTRY}
        WHERE LOWER(COALESCE(feeds_master_columns,'')) LIKE '%todo%'
           OR LOWER(COALESCE(feeds_master_columns,'')) LIKE '%(unset)%'
    """).fetchone()[0])
    log(f"  feeds_master_columns NULL/empty: {n_null}")
    log(f"  feeds_master_columns TODO/(unset): {n_todo}")
    if n_null != 0:
        raise RuntimeError(f"{n_null} registry rows have NULL/empty feeds_master_columns")
    if n_todo != 0:
        raise RuntimeError(f"{n_todo} registry rows contain TODO/(unset)")
    return {"feeds_null": n_null, "feeds_todo": n_todo}


def assert_pointer_resolves(con, log) -> dict:
    """Every detail_table_name referenced by the pointer view must exist."""
    rows = con.execute(f"""
        SELECT DISTINCT detail_table_name, schema_name
        FROM {POINTER}
        WHERE detail_table_name IS NOT NULL
    """).fetchall()
    missing = []
    for tn, sn in rows:
        if not sn:
            sn = "main"
        n = int(con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{PUBLICATION_DB}'
              AND table_schema=? AND table_name=?
              AND table_type IN ('BASE TABLE','VIEW')
        """, [sn, tn]).fetchone()[0])
        if n == 0:
            n_v = int(con.execute(f"""
                SELECT COUNT(*) FROM information_schema.views
                WHERE table_catalog='{PUBLICATION_DB}'
                  AND table_schema=? AND table_name=?
            """, [sn, tn]).fetchone()[0])
            if n_v == 0:
                missing.append(f"{sn}.{tn}")
    log(f"  pointer references {len(rows)} (schema, table) pairs; "
        f"{len(missing)} unresolved")
    for m in missing[:10]:
        log(f"    MISSING {m}")
    if missing:
        raise RuntimeError(f"{len(missing)} pointer tables do not exist: {missing[:5]}")
    return {"n_distinct_tables": len(rows), "n_unresolved": 0}


def assert_priority_drilldowns_mapped(con, log) -> dict:
    """The 3 priority drill-down tables must each have ≥1 CPM column."""
    out = {}
    for tname in PRIORITY_DRILLDOWNS:
        n = int(con.execute(f"""
            SELECT COUNT(DISTINCT master_column) FROM {POINTER}
            WHERE detail_table_name = ?
        """, [tname]).fetchone()[0])
        log(f"  {tname:45s}  CPM cols mapped: {n}")
        out[tname] = n
        if n == 0:
            raise RuntimeError(f"{tname} maps 0 CPM columns in pointer view")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Execute writes; default is dry-run.")
    args = ap.parse_args()
    do_writes = bool(args.apply)
    mode = "APPLY" if do_writes else "DRY-RUN"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log, fh = make_logger(RUN_LOG)
    t0 = time.time()
    log("=" * 78)
    log(f"=== START {Path(__file__).name}  mode={mode}")
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")
    run_ts = utc_ts()
    decision: dict = {
        "script": SCRIPT_NUM, "run_ts": run_ts, "run_date": RUN_DATE,
        "mode": mode, "phases": {},
    }

    try:
        n_reg = int(con.execute(f"SELECT COUNT(*) FROM {REGISTRY}").fetchone()[0])
        n_ptr_before = int(con.execute(
            f"SELECT COUNT(DISTINCT master_column) FROM {POINTER} WHERE detail_table_name IS NOT NULL"
        ).fetchone()[0])
        log(f"PREFLIGHT  registry rows={n_reg}  pointer mapped CPM cols (before)={n_ptr_before}")
        decision["phases"]["preflight"] = {
            "registry_rows": n_reg, "pointer_mapped_before": n_ptr_before,
        }

        if not do_writes:
            log("DRY-RUN — would invoke Script 250 --apply, then assert clean state")
            log(f"=== END  elapsed={time.time()-t0:.1f}s")
            write_decision_log(DECISION_LOG, decision)
            fh.close()
            return

        ensure_archive_schema(con)
        ensure_audit_table(con)

        log("PHASE A — snapshot pointer DDL with pre258 prefix (independent rollback)")
        snap = snapshot_pointer_ddl(con, run_ts, log)
        decision["phases"]["pointer_snapshot"] = snap

        # Close our connection before subprocess so its USE doesn't conflict
        con.close()

        log("PHASE B — invoke Script 250 --apply (registry + pointer rebuild)")
        rc = call_script_250_apply(log)
        decision["phases"]["script_250_rc"] = rc
        if rc != 0:
            raise RuntimeError(f"Script 250 returned non-zero rc={rc}")

        # Reconnect for assertions
        con = connect_locked()
        log("PHASE C — assertions on registry + pointer")
        a = assert_registry_clean(con, log)
        b = assert_pointer_resolves(con, log)
        c = assert_priority_drilldowns_mapped(con, log)
        decision["phases"]["assert_registry"] = a
        decision["phases"]["assert_pointer_resolves"] = b
        decision["phases"]["assert_priority"] = c

        n_ptr_after = int(con.execute(
            f"SELECT COUNT(DISTINCT master_column) FROM {POINTER} WHERE detail_table_name IS NOT NULL"
        ).fetchone()[0])
        log(f"  pointer mapped CPM cols (after): {n_ptr_after}")
        decision["phases"]["pointer_mapped_after"] = n_ptr_after

        record_audit(
            con, SCRIPT_NUM, "criteria_4_5",
            "registry_pointer_health",
            count_before=n_ptr_before, count_after=n_ptr_after,
            target_after=n_ptr_after,
            status="OK",
            notes=(f"feeds_null=0; pointer_unresolved=0; "
                   f"priority_drilldowns_mapped="
                   f"{','.join(f'{k}={v}' for k,v in c.items())}"),
        )

        log("ALL ASSERTIONS PASS")

    except Exception as exc:
        log(f"FATAL: {exc!r}")
        decision["error"] = str(exc)
        write_decision_log(DECISION_LOG, decision)
        fh.close()
        raise

    write_decision_log(DECISION_LOG, decision)
    log(f"=== END  elapsed={time.time()-t0:.1f}s")
    fh.close()


if __name__ == "__main__":
    main()
