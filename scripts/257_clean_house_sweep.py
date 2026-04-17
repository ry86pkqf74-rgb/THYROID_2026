#!/usr/bin/env python3
"""
Script 257 — v1_1 clean-house sweep

Sub-tasks (executed in order, all idempotent):

  (a) Rewrite the 32 manuscript_workspace cohort views that still reference
      `deprecated__tumor_size_cm` / `deprecated__imaging_nodule_size_cm` so
      they source from the canonical successor (`path_tumor_size_cm` /
      `dominant_nodule_size_cm`). The output column name stays the same
      via SQL aliasing — downstream consumers see no surface change.

  (b) Snapshot CPM, then DROP COLUMN for the 9 deprecated columns:
        DEPRECATED__lvi_grade_final_v13
        DEPRECATED__margin_r_class
        DEPRECATED__margin_status_final
        DEPRECATED__max_tumor_size_cm_v10
        DEPRECATED__multifocal_flag
        DEPRECATED__path_multifocal_flag
        DEPRECATED__path_n_tumors
        deprecated__imaging_nodule_size_cm
        deprecated__tumor_size_cm

  (c) Drop `data_dictionary_v240` rows pointing to columns that no longer
      exist in CPM (i.e. status='deprecated' AND target dropped above).

  (d) Sweep `main` for legacy table patterns
      `_backup\\b|_pre\\d+|_predup|_v221|_legacy|_old`. Currently 0 hits;
      this step verifies and would archive any survivors. (No-op expected.)

  (e) Rebuild `main.__readme` and refresh `manuscript_workspace.detail_table_registry_v1`
      row/patient counts using Script 247's queryable enumeration.

Self-verifications at end:
  - 0 views reference any deprecated__ token
  - all 65 ws views compile (SELECT 1 FROM <view> LIMIT 0)
  - CPM column count == before - <number_dropped>
  - CPM rows still 10,871
  - 0 main tables match the legacy patterns

Default --dry-run; pass --apply.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402
from _v1_1_helpers import (  # noqa: E402
    ARCHIVE_QUALIFIED, ensure_archive_schema, ensure_audit_table,
    make_logger, record_audit, snapshot_table, utc_ts, write_decision_log,
)

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
RUN_LOG = OUTPUT_DIR / "257_run.log"
DECISION_LOG = OUTPUT_DIR / "257_decision_log.json"
SCRIPT_TAG = "Script 257"
SCRIPT_NUM = "257"
RUN_DATE = "2026-04-16"

CPM = f'{PUBLICATION_DB}.main.canonical_patient_master'
DICT = f'{PUBLICATION_DB}.main.data_dictionary_v240'
README = f'{PUBLICATION_DB}.main.__readme'
REGISTRY = f'{PUBLICATION_DB}.manuscript_workspace.detail_table_registry_v1'

DEPRECATED_TO_SUCCESSOR = {
    "deprecated__tumor_size_cm":          "path_tumor_size_cm",
    "deprecated__imaging_nodule_size_cm": "dominant_nodule_size_cm",
    # The 7 uppercase DEPRECATED__ columns from Script 249 — view refs were
    # already cleaned by 249, so no view rewrite needed; just dropped.
    "DEPRECATED__lvi_grade_final_v13":    "lvi_ordinal_worst",
    "DEPRECATED__margin_r_class":         "r_class_true",
    "DEPRECATED__margin_status_final":    "r_class_true",
    "DEPRECATED__max_tumor_size_cm_v10":  "tumor_size_cm_max",
    "DEPRECATED__multifocal_flag":        "multifocal_flag_path",
    "DEPRECATED__path_multifocal_flag":   "multifocal_flag_path",
    "DEPRECATED__path_n_tumors":          "n_tumors_path",
}

LEGACY_PATTERN = re.compile(r"_backup|_pre\d+_|_pre\d+$|_predup|_v221|_legacy|_old$",
                            re.IGNORECASE)


# ---------------------------------------------------------------------------
# (a) view rewrite
# ---------------------------------------------------------------------------

# Two patterns we need to substitute. Both occur in Script 240's rewritten
# DDL as `<deprecated_col> AS <original_col>` projections.
VIEW_SUBS = [
    # (regex, replacement)
    (re.compile(r"\bdeprecated__tumor_size_cm\b\s+AS\s+tumor_size_cm",
                re.IGNORECASE),
     "path_tumor_size_cm AS tumor_size_cm"),
    (re.compile(r"\bdeprecated__imaging_nodule_size_cm\b\s+AS\s+imaging_nodule_size_cm",
                re.IGNORECASE),
     "dominant_nodule_size_cm AS imaging_nodule_size_cm"),
    # Bare token (no alias) — replace with the successor (will change output
    # column name; only used internally so safe).
    (re.compile(r"\bdeprecated__tumor_size_cm\b", re.IGNORECASE),
     "path_tumor_size_cm"),
    (re.compile(r"\bdeprecated__imaging_nodule_size_cm\b", re.IGNORECASE),
     "dominant_nodule_size_cm"),
]


def find_views_referencing_deprecated(con) -> list[tuple[str, str, str]]:
    rows = con.execute(f"""
        SELECT table_schema, table_name, view_definition
        FROM information_schema.views
        WHERE table_catalog = '{PUBLICATION_DB}'
    """).fetchall()
    hits = []
    for s, t, d in rows:
        d = d or ""
        for token in ("deprecated__tumor_size_cm",
                      "deprecated__imaging_nodule_size_cm"):
            if token in d:
                hits.append((s, t, d))
                break
    return hits


def snapshot_view_ddl(con, run_ts: str, log) -> str:
    """Snapshot all view DDLs (main + manuscript_workspace) into archive."""
    ensure_archive_schema(con)
    dest = f"view_ddl_snapshot_pre257_{run_ts}"
    full = f'{ARCHIVE_QUALIFIED}."{dest}"'
    con.execute(f"DROP TABLE IF EXISTS {full}")
    con.execute(f"""
        CREATE TABLE {full} (
            schema_name VARCHAR,
            view_name   VARCHAR,
            view_definition VARCHAR,
            snapshotted_at TIMESTAMP
        )
    """)
    rows = con.execute(f"""
        SELECT table_schema, table_name, view_definition
        FROM information_schema.views
        WHERE table_catalog = '{PUBLICATION_DB}'
          AND table_schema IN ('main', 'manuscript_workspace')
        ORDER BY table_schema, table_name
    """).fetchall()
    for s, t, d in rows:
        con.execute(
            f"INSERT INTO {full} VALUES (?, ?, ?, current_timestamp)",
            [s, t, d],
        )
    con.execute(
        f"COMMENT ON TABLE {full} IS '{SCRIPT_TAG} ({RUN_DATE}) "
        f"pre-rewrite snapshot of all main + manuscript_workspace view DDLs.'"
    )
    log(f"  view DDL archive: {full} ({len(rows)} rows)")
    return full


def rewrite_views(con, do_writes: bool, log) -> dict:
    hits = find_views_referencing_deprecated(con)
    log(f"  {len(hits)} views currently reference deprecated__ columns")
    rewrites = []
    for s, t, defn in hits:
        new_def = defn
        for pat, repl in VIEW_SUBS:
            new_def = pat.sub(repl, new_def)
        if new_def == defn:
            log(f"  WARN no substitution made for {s}.{t}")
            continue
        rewrites.append((s, t, defn, new_def))

    if not do_writes:
        for s, t, _, _ in rewrites[:5]:
            log(f"    WOULD rewrite {s}.{t}")
        if len(rewrites) > 5:
            log(f"    ... and {len(rewrites) - 5} more")
        return {"n_to_rewrite": len(rewrites)}

    # Apply: DROP VIEW; CREATE the rewritten DDL.
    for s, t, _, new_def in rewrites:
        full_name = f'"{PUBLICATION_DB}"."{s}"."{t}"'
        # CREATE VIEW DDL captured includes its own CREATE VIEW prefix.
        # Drop + execute the captured-but-rewritten DDL.
        con.execute(f"DROP VIEW IF EXISTS {full_name}")
        con.execute(new_def)
    log(f"  rewrote {len(rewrites)} views")
    return {"n_rewritten": len(rewrites)}


def compile_check_ws_views(con, log) -> tuple[int, list[str]]:
    """SELECT 1 FROM <view> LIMIT 0 across every ws view; report failures."""
    views = con.execute(f"""
        SELECT table_name FROM information_schema.views
        WHERE table_catalog = '{PUBLICATION_DB}'
          AND table_schema = 'manuscript_workspace'
        ORDER BY table_name
    """).fetchall()
    failed = []
    for (vn,) in views:
        try:
            con.execute(
                f'SELECT 1 FROM "{PUBLICATION_DB}"."manuscript_workspace"."{vn}" LIMIT 0'
            )
        except Exception as e:
            failed.append(f"{vn}: {str(e)[:120]}")
    log(f"  ws view compile-test: {len(views) - len(failed)}/{len(views)} OK")
    for f in failed:
        log(f"    FAIL {f}")
    return len(views), failed


# ---------------------------------------------------------------------------
# (b) drop deprecated CPM columns
# ---------------------------------------------------------------------------

def existing_deprecated_cols(con) -> list[str]:
    rows = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
          AND (column_name LIKE 'DEPRECATED__%' OR column_name LIKE 'deprecated__%')
        ORDER BY column_name
    """).fetchall()
    return [r[0] for r in rows]


def drop_deprecated_columns(con, do_writes: bool, log) -> dict:
    cols = existing_deprecated_cols(con)
    log(f"  CPM has {len(cols)} deprecated columns:")
    for c in cols:
        log(f"    - {c}")
    if not do_writes:
        return {"n_to_drop": len(cols), "cols": cols}
    dropped = []
    for c in cols:
        # DuckDB requires one DROP per statement
        con.execute(f'ALTER TABLE {CPM} DROP COLUMN "{c}"')
        dropped.append(c)
    log(f"  dropped {len(dropped)} columns")
    return {"n_dropped": len(dropped), "cols": dropped}


# ---------------------------------------------------------------------------
# (c) prune dictionary
# ---------------------------------------------------------------------------

def prune_dictionary(con, do_writes: bool, log) -> dict:
    cpm_cols = {r[0] for r in con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
    """).fetchall()}
    rows = con.execute(f"""
        SELECT column_name FROM {DICT}
        WHERE status='deprecated'
    """).fetchall()
    to_drop = [r[0] for r in rows if r[0] not in cpm_cols]
    log(f"  dict deprecated rows pointing to dropped CPM cols: {len(to_drop)}")
    for c in to_drop:
        log(f"    - {c}")
    if not do_writes or not to_drop:
        return {"n_to_drop": len(to_drop), "cols": to_drop}
    placeholders = ",".join(["?"] * len(to_drop))
    con.execute(f"DELETE FROM {DICT} WHERE column_name IN ({placeholders})", to_drop)
    return {"n_dropped": len(to_drop), "cols": to_drop}


# ---------------------------------------------------------------------------
# (d) main legacy table sweep (currently 0 hits)
# ---------------------------------------------------------------------------

def legacy_main_table_sweep(con, do_writes: bool, log) -> dict:
    rows = con.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_type='BASE TABLE'
    """).fetchall()
    hits = [r[0] for r in rows if LEGACY_PATTERN.search(r[0])]
    log(f"  main BASE TABLEs matching legacy patterns: {len(hits)}")
    for h in hits:
        log(f"    - {h}")
    if not do_writes or not hits:
        return {"n_hits": len(hits), "tables": hits}
    moved = []
    for tbl in hits:
        run_ts = utc_ts()
        dest = f"{tbl}_main_sweep_pre257_{run_ts}"
        snapshot_table(
            con, f'{PUBLICATION_DB}.main."{tbl}"', dest, SCRIPT_TAG,
            f"Legacy table sweep — pattern matched in {tbl}.",
        )
        con.execute(f'DROP TABLE {PUBLICATION_DB}.main."{tbl}"')
        moved.append({"table": tbl, "archive": dest})
    return {"n_moved": len(moved), "moves": moved}


# ---------------------------------------------------------------------------
# (e) rebuild __readme + refresh detail_table_registry_v1 row counts
# ---------------------------------------------------------------------------

def queryable_main_tables(con, log) -> list[str]:
    """Return main BASE TABLE names that survive a guarded SELECT probe."""
    cands = [r[0] for r in con.execute(f"""
        SELECT table_name FROM information_schema.tables
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_type='BASE TABLE'
        ORDER BY table_name
    """).fetchall()]
    queryable = []
    ghosts = []
    for t in cands:
        try:
            con.execute(f'SELECT 1 FROM main."{t}" LIMIT 0')
            queryable.append(t)
        except Exception as e:
            ghosts.append((t, str(e)[:80]))
    if ghosts:
        log(f"  ghosts (catalog-only, not queryable): {len(ghosts)}")
        for t, e in ghosts[:5]:
            log(f"    - {t}: {e}")
    return queryable


def rebuild_readme(con, queryable: list[str], do_writes: bool, log) -> int:
    if not do_writes:
        log(f"  WOULD rebuild __readme with {len(queryable)} table rows")
        return len(queryable)
    # Snapshot existing __readme first
    run_ts = utc_ts()
    snapshot_table(
        con, README, f"__readme_pre257_{run_ts}", SCRIPT_TAG,
        "Pre-rebuild snapshot of main.__readme (Script 257 sweep).",
    )
    con.execute(f"DROP TABLE IF EXISTS {README}")
    con.execute(f"""
        CREATE TABLE {README} (
            table_name VARCHAR PRIMARY KEY,
            n_rows BIGINT,
            n_distinct_research_id BIGINT,
            description VARCHAR,
            inventoried_at TIMESTAMP
        )
    """)
    n_inserted = 0
    for tbl in queryable:
        n_rows = int(con.execute(f'SELECT COUNT(*) FROM main."{tbl}"').fetchone()[0])
        try:
            n_pat = int(con.execute(
                f'SELECT COUNT(DISTINCT research_id) FROM main."{tbl}"'
            ).fetchone()[0])
        except Exception:
            n_pat = None
        con.execute(
            f"INSERT INTO {README} VALUES (?, ?, ?, ?, current_timestamp)",
            [tbl, n_rows, n_pat,
             f"{SCRIPT_TAG} re-inventoried {RUN_DATE}"],
        )
        n_inserted += 1
    con.execute(
        f"COMMENT ON TABLE {README} IS '{SCRIPT_TAG} ({RUN_DATE}) "
        f"queryable enumeration of main BASE TABLEs ({n_inserted}). "
        f"Auto-rebuilt by Script 257.'"
    )
    log(f"  rebuilt {README} with {n_inserted} rows")
    return n_inserted


def refresh_registry_counts(con, do_writes: bool, log) -> int:
    """Refresh total_rows + total_patients on detail_table_registry_v1
    rows whose detail_table_name resolves to a queryable table."""
    rows = con.execute(f"""
        SELECT detail_table_name, schema_name FROM {REGISTRY}
        WHERE schema_name='main'
    """).fetchall()
    if not do_writes:
        log(f"  WOULD refresh row/patient counts on {len(rows)} registry rows")
        return len(rows)
    n_updated = 0
    for tname, sname in rows:
        try:
            n_rows = int(con.execute(
                f'SELECT COUNT(*) FROM "{sname}"."{tname}"'
            ).fetchone()[0])
        except Exception:
            continue
        try:
            n_pat = int(con.execute(
                f'SELECT COUNT(DISTINCT research_id) FROM "{sname}"."{tname}"'
            ).fetchone()[0])
        except Exception:
            n_pat = None
        con.execute(
            f"UPDATE {REGISTRY} SET total_rows = ?, total_patients = ? "
            f"WHERE detail_table_name = ? AND schema_name = ?",
            [n_rows, n_pat, tname, sname],
        )
        n_updated += 1
    log(f"  refreshed {n_updated} registry rows")
    return n_updated


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

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
        n_cpm = int(con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0])
        n_cpm_cols_before = int(con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='canonical_patient_master'
        """).fetchone()[0])
        if n_cpm != 10871:
            raise RuntimeError(f"CPM rows {n_cpm} != 10871")
        log(f"PREFLIGHT  CPM rows={n_cpm}  CPM cols={n_cpm_cols_before}")
        decision["phases"]["preflight"] = {
            "cpm_rows": n_cpm, "cpm_cols_before": n_cpm_cols_before,
        }

        # --- (a) view rewrite ---
        log("PHASE A — rewrite views referencing deprecated__ columns")
        if do_writes:
            ensure_archive_schema(con)
            ensure_audit_table(con)
            view_archive = snapshot_view_ddl(con, run_ts, log)
            decision["phases"]["view_ddl_archive"] = view_archive
            # Snapshot CPM before any column drops
            cpm_snap = f"canonical_patient_master_pre257_{run_ts}"
            cpm_full = snapshot_table(
                con, CPM, cpm_snap, SCRIPT_TAG,
                "Pre-DROP snapshot of canonical_patient_master before "
                "removing 9 deprecated columns + view rewrite.",
            )
            decision["phases"]["cpm_snapshot"] = cpm_full
            log(f"  CPM snapshot: {cpm_full}")
        rewrite_res = rewrite_views(con, do_writes, log)
        decision["phases"]["view_rewrite"] = rewrite_res

        # --- (b) drop columns ---
        log("PHASE B — drop deprecated CPM columns")
        drop_res = drop_deprecated_columns(con, do_writes, log)
        decision["phases"]["drop_columns"] = drop_res

        # --- (c) dictionary prune ---
        log("PHASE C — prune dictionary rows pointing to dropped columns")
        prune_res = prune_dictionary(con, do_writes, log)
        decision["phases"]["dict_prune"] = prune_res

        # --- (d) main legacy sweep ---
        log("PHASE D — sweep main for legacy table patterns")
        sweep_res = legacy_main_table_sweep(con, do_writes, log)
        decision["phases"]["main_sweep"] = sweep_res

        # --- (e) rebuild __readme + refresh registry ---
        log("PHASE E — rebuild __readme + refresh registry counts")
        queryable = queryable_main_tables(con, log)
        n_readme = rebuild_readme(con, queryable, do_writes, log)
        n_reg = refresh_registry_counts(con, do_writes, log)
        decision["phases"]["readme"] = {"n_rows": n_readme}
        decision["phases"]["registry_refresh"] = {"n_rows": n_reg}

        if not do_writes:
            log("DRY-RUN — assertions skipped")
            log(f"=== END  elapsed={time.time()-t0:.1f}s")
            write_decision_log(DECISION_LOG, decision)
            fh.close()
            return

        # --- assertions ---
        log("ASSERT 1 — 0 views reference deprecated__ tokens")
        leftover = find_views_referencing_deprecated(con)
        if leftover:
            log(f"  FAIL: {len(leftover)} views still reference deprecated__")
            for s, t, _ in leftover[:5]:
                log(f"    - {s}.{t}")
            raise RuntimeError(f"{len(leftover)} views still reference deprecated__")
        log("  OK: 0 leftover refs")

        log("ASSERT 2 — all ws views compile")
        n_views, failed = compile_check_ws_views(con, log)
        if failed:
            raise RuntimeError(f"{len(failed)} ws views failed compile-test: {failed[:3]}")

        log("ASSERT 3 — CPM column count dropped by exactly 9")
        n_cpm_cols_after = int(con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='canonical_patient_master'
        """).fetchone()[0])
        delta = n_cpm_cols_before - n_cpm_cols_after
        decision["phases"]["cpm_cols_after"] = n_cpm_cols_after
        decision["phases"]["cpm_cols_delta"] = delta
        log(f"  before={n_cpm_cols_before} after={n_cpm_cols_after} delta={delta}")
        if delta != len(DEPRECATED_TO_SUCCESSOR):
            raise RuntimeError(
                f"CPM cols dropped {delta} != expected {len(DEPRECATED_TO_SUCCESSOR)}"
            )

        log("ASSERT 4 — CPM rows still 10,871")
        n_cpm_after = int(con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0])
        if n_cpm_after != 10871:
            raise RuntimeError(f"CPM rows {n_cpm_after} != 10871")

        log("ASSERT 5 — main has 0 legacy-pattern tables")
        residual = [r[0] for r in con.execute(f"""
            SELECT table_name FROM information_schema.tables
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_type='BASE TABLE'
        """).fetchall() if LEGACY_PATTERN.search(r[0])]
        if residual:
            raise RuntimeError(f"legacy tables still present: {residual}")
        log(f"  OK: 0 legacy-pattern tables in main")

        log("ASSERT 6 — 0 dict rows status='deprecated' point to existing CPM column")
        n_dict_dep_living = int(con.execute(f"""
            WITH cpm_cols AS (
              SELECT column_name FROM information_schema.columns
              WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                AND table_name='canonical_patient_master'
            )
            SELECT COUNT(*) FROM {DICT} d
            WHERE d.status='deprecated'
              AND d.column_name IN (SELECT column_name FROM cpm_cols)
        """).fetchone()[0])
        if n_dict_dep_living != 0:
            raise RuntimeError(
                f"{n_dict_dep_living} dict deprecated rows still point to live CPM cols"
            )
        log("  OK: 0 such rows")

        record_audit(
            con, SCRIPT_NUM, "criteria_1_2_4_5",
            "clean_house_summary",
            count_before=n_cpm_cols_before, count_after=n_cpm_cols_after,
            target_after=n_cpm_cols_before - len(DEPRECATED_TO_SUCCESSOR),
            status="OK",
            notes=(f"deprecated_cols_dropped={delta}; legacy_tables_residual=0; "
                   f"ws_views_compile_pass={n_views}; "
                   f"readme_rows={n_readme}; registry_refreshed={n_reg}"),
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
