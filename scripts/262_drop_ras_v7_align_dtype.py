#!/usr/bin/env python3
"""
Script 262 - Drop ras_positive_v7 from CPM + align fna_episode_master_v2.research_id dtype

Per Prompt 13:
  Step 1-5: ras_positive_v7 is the last legacy versioned-with-successor column on
            canonical_patient_master (successor: ras_positive_v11). The audit
            sweep table manuscript_workspace.legacy_column_sweep_v1_1 has 1 row
            pointing at it.
  Step 6:   fna_episode_master_v2.research_id is INTEGER while CPM.research_id is
            VARCHAR; this forces CAST on every join. Cast FEM column to VARCHAR
            and confirm row count + join behavior unchanged.

Snapshots:
  CPM ->                  archive_pub_v1_0.canonical_patient_master_pre262_<tsZ>
  ras_v7 column ->        archive_pub_v1_0.cpm_ras_positive_v7_dropped_<tsZ>
                          (2-col: research_id, ras_positive_v7)
  FEM ->                  archive_pub_v1_0.fna_episode_master_v2_pre262_<tsZ>

Mutations:
  ALTER TABLE canonical_patient_master DROP COLUMN ras_positive_v7;
  UPDATE data_dictionary_v240 SET status='removed', replacement_column_name='ras_positive_v11'
    WHERE column_name='ras_positive_v7';
  Re-build manuscript_workspace.legacy_column_sweep_v1_1 (expected 0 rows after).
  ALTER TABLE fna_episode_master_v2 ALTER research_id TYPE VARCHAR;

Invariants:
  CPM column count = pre - 1 (= 1,493 expected from prompt).
  FEM.research_id is VARCHAR.
  FEM rowcount unchanged (8,119).
  CPM JOIN FEM on research_id without CAST returns the same count as the
    pre-mutation CAST-based join.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import (  # noqa: E402
    ensure_audit_table, ensure_archive_schema, make_logger,
    record_audit, snapshot_table, utc_ts, write_decision_log,
    ARCHIVE_QUALIFIED,
)

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
RUN_LOG = OUTPUT_DIR / "262_run.log"
DECISION_LOG = OUTPUT_DIR / "262_decision_log.json"
SCRIPT_TAG = "Script 262"
SCRIPT_NUM = "262"
RUN_DATE = "2026-04-17"

CPM = f'{PUBLICATION_DB}.main.canonical_patient_master'
FEM = f'{PUBLICATION_DB}.main.fna_episode_master_v2'
DICT = f'{PUBLICATION_DB}.main.data_dictionary_v240'
SWEEP = f'{PUBLICATION_DB}.manuscript_workspace.legacy_column_sweep_v1_1'

EXPECTED_CPM_ROWS = 10871
EXPECTED_FEM_ROWS = 8119


def cpm_col_count(con) -> int:
    return int(con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
    """).fetchone()[0])


def has_column(con, table_name: str, column_name: str) -> bool:
    return con.execute(f"""
        SELECT 1 FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name=? AND column_name=?
    """, [table_name, column_name]).fetchone() is not None


def get_dtype(con, table_name: str, column_name: str) -> str | None:
    row = con.execute(f"""
        SELECT data_type FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name=? AND column_name=?
    """, [table_name, column_name]).fetchone()
    return row[0] if row else None


def find_views_referencing(con, token: str) -> list[tuple[str, str]]:
    rows = con.execute(f"""
        SELECT table_schema, table_name FROM information_schema.views
        WHERE table_catalog='{PUBLICATION_DB}'
          AND view_definition ILIKE ?
    """, [f"%{token}%"]).fetchall()
    return [(r[0], r[1]) for r in rows]


def rebuild_legacy_sweep(con, log) -> int:
    """Recompute manuscript_workspace.legacy_column_sweep_v1_1 against current
       CPM columns. Returns post-rebuild row count."""
    sql = f"""
    CREATE OR REPLACE TABLE {SWEEP} AS
    WITH cols AS (
      SELECT column_name FROM information_schema.columns
       WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
         AND table_name='canonical_patient_master'
    ),
    versioned AS (
      SELECT column_name,
             CAST(regexp_extract(column_name, '_v(\\d+)$', 1) AS INTEGER) AS version,
             regexp_replace(column_name, '_v\\d+$', '') AS stem
        FROM cols
       WHERE column_name ~ '_v\\d+$'
    ),
    versioned_max AS (
      SELECT stem, MAX(version) AS max_version_in_cpm FROM versioned GROUP BY stem
    )
    SELECT v.column_name, v.version, v.stem,
           m.max_version_in_cpm,
           v.stem || '_v' || CAST(m.max_version_in_cpm AS VARCHAR) AS successor_column,
           CURRENT_TIMESTAMP AS inventoried_at
      FROM versioned v
      JOIN versioned_max m ON v.stem = m.stem
     WHERE v.version < m.max_version_in_cpm
    ORDER BY v.column_name
    """
    con.execute(sql)
    n = int(con.execute(f"SELECT COUNT(*) FROM {SWEEP}").fetchone()[0])
    log(f"  legacy_column_sweep_v1_1 rebuilt: {n} rows")
    return n


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
        n_cpm_cols_pre = cpm_col_count(con)
        ras_dtype_pre = get_dtype(con, "canonical_patient_master", "ras_positive_v7")
        rid_dtype_pre = get_dtype(con, "fna_episode_master_v2", "research_id")
        n_fem_pre = int(con.execute(f"SELECT COUNT(*) FROM {FEM}").fetchone()[0])

        # Pre-mutation join count using CAST
        cast_join_pre = int(con.execute(f"""
            SELECT COUNT(*) FROM {CPM} cpm
            JOIN {FEM} f ON CAST(f.research_id AS VARCHAR) = cpm.research_id
        """).fetchone()[0])

        log(f"PREFLIGHT  CPM cols={n_cpm_cols_pre}; ras_v7 dtype={ras_dtype_pre}")
        log(f"           FEM rows={n_fem_pre}; FEM.research_id dtype={rid_dtype_pre}")
        log(f"           CPM JOIN FEM (CAST) row count: {cast_join_pre}")

        view_hits = find_views_referencing(con, "ras_positive_v7")
        log(f"  views referencing ras_positive_v7: {len(view_hits)}")
        for s, t in view_hits:
            log(f"    {s}.{t}")
        decision["phases"]["preflight"] = {
            "cpm_cols": n_cpm_cols_pre,
            "ras_v7_dtype": ras_dtype_pre,
            "fem_rid_dtype": rid_dtype_pre,
            "cast_join_pre": cast_join_pre,
            "view_hits_ras_v7": [{"schema": s, "table": t} for s, t in view_hits],
            "fem_rows": n_fem_pre,
        }

        if not do_writes:
            log("DRY-RUN -- no mutations performed")
            log(f"=== END  elapsed={time.time()-t0:.1f}s")
            write_decision_log(DECISION_LOG, decision)
            fh.close()
            return

        ensure_archive_schema(con)
        ensure_audit_table(con)

        # ----- Snapshots -----
        snap_cpm = snapshot_table(
            con, CPM, f"canonical_patient_master_pre262_{run_ts}", SCRIPT_TAG,
            "Pre-mutation snapshot of CPM before DROP COLUMN ras_positive_v7.")
        log(f"SNAPSHOT  cpm = {snap_cpm}")

        ras_snap_name = f"cpm_ras_positive_v7_dropped_{run_ts}"
        ras_snap_full = f'{ARCHIVE_QUALIFIED}."{ras_snap_name}"'
        if ras_dtype_pre is not None:
            con.execute(f"DROP TABLE IF EXISTS {ras_snap_full}")
            con.execute(f"""
                CREATE TABLE {ras_snap_full} AS
                SELECT research_id, ras_positive_v7 FROM {CPM}
            """)
            con.execute(
                f"COMMENT ON TABLE {ras_snap_full} IS "
                f"'{SCRIPT_TAG} ({RUN_DATE}). 2-col archive of CPM.ras_positive_v7 "
                f"prior to DROP COLUMN. Successor column: ras_positive_v11.'"
            )
            log(f"SNAPSHOT  ras_v7 column = {ras_snap_full}")
        else:
            log("  ras_positive_v7 already absent; skipping column snapshot")

        snap_fem = snapshot_table(
            con, FEM, f"fna_episode_master_v2_pre262_{run_ts}", SCRIPT_TAG,
            "Pre-mutation snapshot of FEM before research_id INTEGER -> VARCHAR cast.")
        log(f"SNAPSHOT  fem = {snap_fem}")
        decision["phases"]["snapshots"] = {
            "cpm": snap_cpm, "ras_v7_col": ras_snap_full if ras_dtype_pre else None,
            "fem": snap_fem,
        }

        # ----- DROP ras_positive_v7 -----
        if ras_dtype_pre is not None:
            con.execute(f"ALTER TABLE {CPM} DROP COLUMN ras_positive_v7")
            log("  ALTER TABLE canonical_patient_master DROP COLUMN ras_positive_v7")
        else:
            log("  ras_positive_v7 already dropped; skipping DROP")

        # ----- Update data_dictionary_v240 -----
        n_dict = int(con.execute(
            f"SELECT COUNT(*) FROM {DICT} WHERE column_name='ras_positive_v7'"
        ).fetchone()[0])
        if n_dict:
            con.execute(f"""
                UPDATE {DICT}
                   SET status = 'removed',
                       replacement_column_name = 'ras_positive_v11'
                 WHERE column_name = 'ras_positive_v7'
            """)
            log("  data_dictionary_v240: ras_positive_v7 -> status=removed")
        else:
            log("  data_dictionary_v240: no row for ras_positive_v7")

        # ----- Rebuild legacy_column_sweep_v1_1 -----
        n_sweep = rebuild_legacy_sweep(con, log)
        if n_sweep != 0:
            raise RuntimeError(
                f"legacy_column_sweep_v1_1 has {n_sweep} rows after rebuild; "
                "expected 0. Investigate before continuing."
            )

        # ----- ALTER FEM.research_id INTEGER -> VARCHAR -----
        if rid_dtype_pre and rid_dtype_pre.upper() != "VARCHAR":
            con.execute(
                f"ALTER TABLE {FEM} ALTER research_id TYPE VARCHAR"
            )
            log("  ALTER TABLE fna_episode_master_v2 ALTER research_id TYPE VARCHAR")
        else:
            log("  fna_episode_master_v2.research_id already VARCHAR; skipping ALTER")

        # ----- Invariants -----
        n_cpm_cols_post = cpm_col_count(con)
        ras_dtype_post = get_dtype(con, "canonical_patient_master", "ras_positive_v7")
        rid_dtype_post = get_dtype(con, "fna_episode_master_v2", "research_id")
        n_fem_post = int(con.execute(f"SELECT COUNT(*) FROM {FEM}").fetchone()[0])
        plain_join_post = int(con.execute(f"""
            SELECT COUNT(*) FROM {CPM} cpm
            JOIN {FEM} f ON f.research_id = cpm.research_id
        """).fetchone()[0])

        if ras_dtype_post is not None:
            raise RuntimeError("ras_positive_v7 still present on CPM")
        if rid_dtype_post is None or rid_dtype_post.upper() != "VARCHAR":
            raise RuntimeError(
                f"FEM.research_id dtype is {rid_dtype_post}, expected VARCHAR"
            )
        if n_cpm_cols_post != n_cpm_cols_pre - 1:
            raise RuntimeError(
                f"CPM col count {n_cpm_cols_post} != {n_cpm_cols_pre - 1}"
            )
        if n_fem_post != EXPECTED_FEM_ROWS:
            raise RuntimeError(f"FEM rowcount drifted: {n_fem_post} != {EXPECTED_FEM_ROWS}")
        if plain_join_post != cast_join_pre:
            raise RuntimeError(
                f"CPM-FEM join count post-cast (no CAST) = {plain_join_post} != "
                f"pre-cast CAST-based count {cast_join_pre}"
            )

        log(f"INVARIANTS  CPM cols pre={n_cpm_cols_pre} post={n_cpm_cols_post} "
            f"(target {n_cpm_cols_pre-1})")
        log(f"            FEM rows={n_fem_post}; FEM.research_id={rid_dtype_post}")
        log(f"            CPM JOIN FEM (no CAST) count = {plain_join_post} "
            f"(== pre-cast CAST count {cast_join_pre})")
        decision["phases"]["invariants"] = {
            "cpm_cols_pre": n_cpm_cols_pre,
            "cpm_cols_post": n_cpm_cols_post,
            "ras_v7_dtype_post": ras_dtype_post,
            "fem_rid_dtype_post": rid_dtype_post,
            "fem_rows_post": n_fem_post,
            "plain_join_post": plain_join_post,
            "sweep_rows_post": n_sweep,
        }

        record_audit(
            con, SCRIPT_NUM, "prompt13_ras_v7_drop",
            "cpm_col_count_delta",
            count_before=n_cpm_cols_pre, count_after=n_cpm_cols_post,
            target_after=n_cpm_cols_pre - 1, status="OK",
            notes=f"snap_cpm={snap_cpm}; ras_snap={ras_snap_full}; "
                  f"sweep_rows_post={n_sweep}",
        )
        record_audit(
            con, SCRIPT_NUM, "prompt13_fem_rid_dtype",
            "fem_research_id_dtype",
            count_before=0, count_after=1,
            target_after=1, status="OK",
            notes=f"pre={rid_dtype_pre} post={rid_dtype_post}; "
                  f"join count {plain_join_post}",
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
