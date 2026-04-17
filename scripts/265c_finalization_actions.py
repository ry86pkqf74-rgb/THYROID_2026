#!/usr/bin/env python3
"""
Script 265c - follow-up actions after 265 / 265b review

(1) Variance #5 correction: vc_paralysis_recalibration_v236 lives in
    manuscript_workspace, not main. Update the 3 collision_resolution_v265 rows
    (comp_rln_injury_confirmed, comp_vc_paralysis_confirmed,
    comp_vc_paresis_confirmed) to use the corrected rationale.

(2) Phase 9b: drop the 12 release_* schemas in 'Thyroid 2026 UPdated'. Snapshot
    metadata only (no data copy - archive_pub_v1_0 already holds the
    comprehensive pre-251 publication snapshot) into
    archive_legacy.release_schemas_manifest_pre265_drop, then DROP each schema
    CASCADE.

Default dry-run; pass --apply to execute.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import (  # noqa: E402
    ARCHIVE_DB, ensure_audit_table, make_logger, record_audit,
    write_decision_log,
)

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG = OUT_DIR / "265c_run.log"
DECISION_LOG = OUT_DIR / "265c_decision_log.json"

SCRIPT_TAG = "Script 265c"
SCRIPT_NUM = "265c"
RUN_DATE = "2026-04-17"

COLLISION_TBL = (
    f"{PUBLICATION_DB}.manuscript_workspace.collision_resolution_v265"
)
ARCHIVE_LEGACY = f'"{ARCHIVE_DB}"."archive_legacy"'
MANIFEST_FQ = f'{ARCHIVE_LEGACY}."release_schemas_manifest_pre265_drop"'

CORRECTED_RATIONALE = (
    "complication_phenotype_v1 covers ~2,938 patients; "
    "vc_paralysis_recalibration_v236 (in manuscript_workspace) is a targeted "
    "59-patient override, not a patient-level feeder. COALESCE applied in CPM "
    "build logic, not in registry pointer."
)
CORRECTED_COLS = (
    "comp_rln_injury_confirmed",
    "comp_vc_paralysis_confirmed",
    "comp_vc_paresis_confirmed",
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Execute writes; default dry-run.")
    args = ap.parse_args()
    do_writes = bool(args.apply)
    log, fh = make_logger(RUN_LOG)
    t0 = time.time()
    try:
        log("=" * 78)
        log(f"=== START scripts/265c_finalization_actions.py "
            f"({'APPLY' if do_writes else 'DRY-RUN'})")
        log(f"started_at: {utc_now()}")
        con = connect_locked()
        log(f"connected to {PUBLICATION_DB}")

        # -------------------------------------------------------------------
        # (1) Rationale correction
        # -------------------------------------------------------------------
        log("\nFIX 1 - collision_resolution_v265 rationale correction")
        before = con.execute(f"""
            SELECT master_column, rationale FROM {COLLISION_TBL}
            WHERE master_column IN ({','.join(repr(c) for c in CORRECTED_COLS)})
            ORDER BY master_column
        """).fetchall()
        for mc, rat in before:
            log(f"  before  {mc}: {rat[:120]}")

        if do_writes:
            for mc in CORRECTED_COLS:
                con.execute(
                    f"UPDATE {COLLISION_TBL} SET rationale = ?, "
                    f"resolved_at = current_timestamp WHERE master_column = ?",
                    [CORRECTED_RATIONALE, mc])
            after = con.execute(f"""
                SELECT master_column, rationale FROM {COLLISION_TBL}
                WHERE master_column IN ({','.join(repr(c) for c in CORRECTED_COLS)})
                ORDER BY master_column
            """).fetchall()
            for mc, rat in after:
                log(f"  after   {mc}: {rat[:160]}")
        else:
            log(f"  DRY-RUN; would update {len(CORRECTED_COLS)} rationale rows")

        # -------------------------------------------------------------------
        # (2) Phase 9b: drop 12 release_* schemas (metadata snapshot first)
        # -------------------------------------------------------------------
        log("\nFIX 2 - Phase 9b: drop release_* schemas")
        release_schemas = sorted(r[0] for r in con.execute(f"""
            SELECT DISTINCT schema_name FROM duckdb_tables()
            WHERE database_name = '{ARCHIVE_DB}'
              AND schema_name LIKE 'release_%'
        """).fetchall())
        log(f"  release_* schemas found: {len(release_schemas)}")
        for s in release_schemas:
            log(f"    {s}")

        n_total_tables = int(con.execute(f"""
            SELECT COUNT(*) FROM duckdb_tables()
            WHERE database_name = '{ARCHIVE_DB}'
              AND schema_name LIKE 'release_%'
        """).fetchone()[0])
        log(f"  total tables across release_* schemas: {n_total_tables}")

        if do_writes:
            con.execute(f'CREATE SCHEMA IF NOT EXISTS {ARCHIVE_LEGACY}')
            con.execute(f"""
                CREATE OR REPLACE TABLE {MANIFEST_FQ} AS
                SELECT
                  t.table_schema,
                  t.table_name,
                  t.table_type,
                  TRUE AS exists_at_drop_time,
                  current_timestamp AS manifest_at
                FROM "{ARCHIVE_DB}".information_schema.tables t
                WHERE table_catalog = '{ARCHIVE_DB}'
                  AND table_schema LIKE 'release_%'
            """)
            n_manifest = con.execute(
                f"SELECT COUNT(*) FROM {MANIFEST_FQ}").fetchone()[0]
            log(f"  metadata snapshot: {MANIFEST_FQ} ({n_manifest} rows)")

            comment = (
                "Script 265c metadata-only snapshot (no data copy) of the 12 "
                "release_* schemas in 'Thyroid 2026 UPdated' that were dropped "
                f"on {RUN_DATE}. archive_pub_v1_0 already holds the "
                "comprehensive pre-251 publication snapshot; if a specific "
                "release artifact is needed, regenerate it from the build "
                "pipeline."
            ).replace("'", "''")
            con.execute(f"COMMENT ON TABLE {MANIFEST_FQ} IS '{comment}'")

            n_dropped = 0
            for s in release_schemas:
                try:
                    con.execute(f'DROP SCHEMA "{ARCHIVE_DB}"."{s}" CASCADE')
                    n_dropped += 1
                    log(f"  DROPPED {ARCHIVE_DB}.{s} CASCADE")
                except Exception as e:
                    log(f"  WARNING - failed to drop {s}: {e}")

            remaining = int(con.execute(f"""
                SELECT COUNT(DISTINCT schema_name) FROM duckdb_tables()
                WHERE database_name = '{ARCHIVE_DB}'
                  AND schema_name LIKE 'release_%'
            """).fetchone()[0])
            remaining_tables = int(con.execute(f"""
                SELECT COUNT(*) FROM duckdb_tables()
                WHERE database_name = '{ARCHIVE_DB}'
                  AND schema_name LIKE 'release_%'
            """).fetchone()[0])
            log(f"  remaining release_* schemas: {remaining}  "
                f"tables: {remaining_tables}")

            ensure_audit_table(con)
            record_audit(
                con, SCRIPT_NUM, "phase_9b_release_schemas_dropped",
                "release_schemas_remaining",
                count_before=len(release_schemas),
                count_after=remaining, target_after=0,
                status="OK" if remaining == 0 else "PARTIAL",
                notes=(f"Dropped {n_dropped} schemas / "
                       f"{n_total_tables - remaining_tables} tables. "
                       f"Manifest at {MANIFEST_FQ}."))
            record_audit(
                con, SCRIPT_NUM, "collision_rationale_correction",
                "rows_corrected",
                count_before=len(CORRECTED_COLS),
                count_after=len(CORRECTED_COLS), target_after=len(CORRECTED_COLS),
                status="OK",
                notes="vc_paralysis_recalibration_v236 lives in manuscript_workspace, not main; rationale corrected.")
            log("  audit rows written")
            summary = {
                "release_dropped": n_dropped,
                "release_remaining": remaining,
                "rationale_rows_corrected": len(CORRECTED_COLS),
                "manifest_rows": n_manifest,
            }
        else:
            log(f"  DRY-RUN; would snapshot metadata + drop "
                f"{len(release_schemas)} schemas / {n_total_tables} tables")
            summary = {
                "release_dropped": 0, "release_remaining": len(release_schemas),
                "rationale_rows_corrected": 0,
                "manifest_rows": n_total_tables,
            }

        elapsed = time.time() - t0
        log(f"=== END elapsed={elapsed:.1f}s")
        write_decision_log(DECISION_LOG, {
            "script": SCRIPT_TAG, "run_date": RUN_DATE,
            "do_writes": do_writes, "elapsed_seconds": round(elapsed, 1),
            "release_schemas_targeted": release_schemas,
            "summary": summary,
        })
        return 0
    except Exception as e:
        log(f"FATAL: {e!r}")
        import traceback
        log(traceback.format_exc())
        return 1
    finally:
        fh.close()


if __name__ == "__main__":
    sys.exit(main())
