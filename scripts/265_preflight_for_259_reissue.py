#!/usr/bin/env python3
"""
Script 265 — Preflight assessment for the Script 259 (re-issued) prompt.

This is a READ-ONLY probe. It checks whether the repo has already executed
each "step" the user's stale prompt asks for, and surfaces the current state
so we can decide scope before any mutating work begins.

NEVER writes to either DB.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = OUT_DIR / "265_preflight.log"


def main() -> int:
    fh = LOG_PATH.open("w", encoding="utf-8")

    def log(msg: str) -> None:
        line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
        print(line, flush=True)
        fh.write(line + "\n")
        fh.flush()

    log("=" * 78)
    log("Preflight for Script 259 reissue (read-only)")
    log("=" * 78)
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    # ----- Block A: prompt's pre-flight check -----
    log("\n--- Block A: prompt pre-flight ---")
    row = con.execute(f"""
        SELECT
          (SELECT COUNT(*) FROM duckdb_tables() WHERE database_name='{PUBLICATION_DB}'
                 AND schema_name='main') AS main_n_tables,
          (SELECT COUNT(*) FROM duckdb_tables() WHERE database_name='{PUBLICATION_DB}'
                 AND schema_name='manuscript_workspace') AS ws_n_tables,
          (SELECT COUNT(*) FROM {PUBLICATION_DB}.main.canonical_patient_master) AS n_patients,
          (SELECT COUNT(*) FROM information_schema.columns
                 WHERE table_catalog='{PUBLICATION_DB}'
                 AND table_schema='main'
                 AND table_name='canonical_patient_master') AS n_cols,
          (SELECT COUNT(*) FROM {PUBLICATION_DB}.main.__readme) AS readme_rows,
          (SELECT COUNT(*) FROM {PUBLICATION_DB}.manuscript_workspace.detail_table_registry_v1) AS registry_rows
    """).fetchone()
    log(f"main_n_tables={row[0]}  ws_n_tables={row[1]}  n_patients={row[2]}  "
        f"n_cols={row[3]}  readme_rows={row[4]}  registry_rows={row[5]}")
    log(f"prompt expected: main_n_tables=114, ws_n_tables>=10, n_patients=10871, "
        f"n_cols=1494, readme_rows=114, registry_rows=116")

    # ----- Block B: per-step status probes -----
    log("\n--- Block B: per-step probes ---")

    # Step 1: ras_positive_v7
    log("\n[Step 1] ras_positive_v7 column status")
    has_v7 = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master' AND column_name='ras_positive_v7'
    """).fetchone()[0]
    log(f"  ras_positive_v7 column present: {bool(has_v7)} (expected after 262: False)")

    # Step 2: registry unmatched tokens
    log("\n[Step 2] registry normalization review")
    try:
        norm_rows = con.execute(
            f"SELECT COUNT(*) FROM {PUBLICATION_DB}.manuscript_workspace.registry_normalization_review_v1_1"
        ).fetchone()[0]
        log(f"  registry_normalization_review_v1_1 row count: {norm_rows}")
        unmatched = con.execute(f"""
            SELECT detail_table_name, n_explicit_unmatched
            FROM {PUBLICATION_DB}.manuscript_workspace.registry_normalization_review_v1_1
            WHERE n_explicit_unmatched > 0
            ORDER BY detail_table_name
        """).fetchall()
        log(f"  rows with n_explicit_unmatched>0: {len(unmatched)}")
        for r in unmatched:
            log(f"    {r[0]}  unmatched={r[1]}")
    except Exception as e:
        log(f"  ERROR reading registry_normalization_review_v1_1: {e}")

    # Step 3: collisions
    log("\n[Step 3] collision count via canonical_detail_pointer_v1")
    try:
        coll = con.execute(f"""
            WITH per_col AS (
              SELECT master_column, COUNT(DISTINCT detail_table_name) AS n_feeders
              FROM {PUBLICATION_DB}.manuscript_workspace.canonical_detail_pointer_v1
              GROUP BY master_column
            )
            SELECT
              COUNT(*) AS distinct_master_cols,
              COUNT(*) FILTER (WHERE n_feeders > 1) AS colliding_master_cols,
              MAX(n_feeders) AS worst_feeder_count
            FROM per_col
        """).fetchone()
        log(f"  distinct_master_cols={coll[0]}  colliding={coll[1]}  worst_feeder_count={coll[2]}")
    except Exception as e:
        log(f"  ERROR: {e}")

    try:
        ptr_rows = con.execute(
            f"SELECT COUNT(*) FROM {PUBLICATION_DB}.manuscript_workspace.canonical_detail_pointer_v1"
        ).fetchone()[0]
        log(f"  canonical_detail_pointer_v1 total rows: {ptr_rows}")
    except Exception as e:
        log(f"  ERROR row count: {e}")

    coll_table = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}'
          AND table_schema='manuscript_workspace'
          AND table_name='collision_resolution_v259'
    """).fetchone()[0]
    log(f"  collision_resolution_v259 table exists: {bool(coll_table)}")

    # Step 4: molecular_test_episode_v2 platform breakdown
    log("\n[Step 4] molecular_test_episode_v2 platform breakdown")
    try:
        plats = con.execute(f"""
            SELECT platform, COUNT(*) FROM {PUBLICATION_DB}.main.molecular_test_episode_v2
            GROUP BY platform ORDER BY 2 DESC
        """).fetchall()
        for p in plats:
            log(f"  platform={p[0]!r:<20} n={p[1]}")
    except Exception as e:
        log(f"  ERROR: {e}")

    # Step 5: fusion contradiction
    log("\n[Step 5] any_fusion_positive vs mol_n_fusions contradictions")
    try:
        contra = con.execute(f"""
            SELECT
              COUNT(*) FILTER (WHERE mol_n_fusions > 0 AND any_fusion_positive = FALSE) AS contradiction_a,
              COUNT(*) FILTER (WHERE mol_n_fusions = 0 AND any_fusion_positive = TRUE)  AS contradiction_b
            FROM {PUBLICATION_DB}.main.canonical_patient_master
        """).fetchone()
        log(f"  contradiction_a (n_fusions>0 & flag=FALSE): {contra[0]}")
        log(f"  contradiction_b (n_fusions=0 & flag=TRUE):  {contra[1]}")
    except Exception as e:
        log(f"  ERROR: {e}")

    # Step 6: NULL fusion / RET
    log("\n[Step 6] NULL fusion / RET flags")
    try:
        nulls = con.execute(f"""
            SELECT
              COUNT(*) FILTER (WHERE any_fusion_positive IS NULL) AS null_fusion,
              COUNT(*) FILTER (WHERE ret_positive_v7 IS NULL) AS null_ret
            FROM {PUBLICATION_DB}.main.canonical_patient_master
        """).fetchone()
        log(f"  null_fusion={nulls[0]}  null_ret={nulls[1]}")
        for cn in ("any_fusion_positive_inferred_negative", "ret_positive_v7_inferred_negative"):
            present = con.execute(f"""
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                  AND table_name='canonical_patient_master' AND column_name='{cn}'
            """).fetchone()[0]
            log(f"  column {cn} present: {bool(present)}")
    except Exception as e:
        log(f"  ERROR: {e}")

    # Step 7: ghost RID 7744
    log("\n[Step 7] ghost RID 7744")
    try:
        for src in ("canonical_patient_master", "canonical_molecular_tested_v1",
                    "molecular_test_episode_v2", "molecular_results", "molecular_testing",
                    "thyroseq_molecular_enrichment", "_molecular_patient_rollup_v227"):
            try:
                n = con.execute(
                    f"SELECT COUNT(*) FROM {PUBLICATION_DB}.main.{src} "
                    f"WHERE TRY_CAST(research_id AS INTEGER)=7744"
                ).fetchone()[0]
            except Exception as e:
                n = f"err:{e.__class__.__name__}"
            log(f"  {src:<40}  n_rows_for_rid_7744={n}")
    except Exception as e:
        log(f"  ERROR: {e}")

    # Step 8: nan strings audit
    log("\n[Step 8] nan_string_audit_v1_1 status")
    try:
        nan_audit = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
              AND table_name='nan_string_audit_v1_1'
        """).fetchone()[0]
        log(f"  nan_string_audit_v1_1 table exists: {bool(nan_audit)}")
        if nan_audit:
            n = con.execute(
                f"SELECT COUNT(*) FROM {PUBLICATION_DB}.manuscript_workspace.nan_string_audit_v1_1"
            ).fetchone()[0]
            log(f"  nan_string_audit_v1_1 row count: {n}")
            cols = [r[0] for r in con.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
                  AND table_name='nan_string_audit_v1_1'
                ORDER BY ordinal_position
            """).fetchall()]
            log(f"  nan_string_audit_v1_1 columns: {cols}")
    except Exception as e:
        log(f"  ERROR: {e}")

    # Step 9: archive DB inventory
    log("\n[Step 9] archive DB inventory ('Thyroid 2026 UPdated')")
    try:
        schemas = con.execute("""
            SELECT schema_name, COUNT(*) AS n_tables
            FROM duckdb_tables()
            WHERE database_name = 'Thyroid 2026 UPdated'
            GROUP BY schema_name ORDER BY 2 DESC
        """).fetchall()
        for s in schemas:
            log(f"  schema={s[0]:<30}  n_tables={s[1]}")
    except Exception as e:
        log(f"  ERROR: {e}")

    # Step 10/11: views + readme
    log("\n[Step 10/11] views + readme")
    try:
        n_views = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
              AND table_type='VIEW'
        """).fetchone()[0]
        log(f"  manuscript_workspace VIEW count: {n_views}")
        n_readme = con.execute(
            f"SELECT COUNT(*) FROM {PUBLICATION_DB}.main.__readme"
        ).fetchone()[0]
        log(f"  __readme rows: {n_readme}")
    except Exception as e:
        log(f"  ERROR: {e}")

    # Audit table state
    log("\n[Audit] v1_1_finalization_audit_v1 latest by script")
    try:
        rows = con.execute(f"""
            SELECT script_num, COUNT(*), MAX(run_ts)
            FROM {PUBLICATION_DB}.manuscript_workspace.v1_1_finalization_audit_v1
            GROUP BY script_num ORDER BY 1
        """).fetchall()
        for r in rows:
            log(f"  script_num={r[0]:<6}  n={r[1]}  latest={r[2]}")
    except Exception as e:
        log(f"  ERROR: {e}")

    log("\nPreflight complete.")
    fh.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
