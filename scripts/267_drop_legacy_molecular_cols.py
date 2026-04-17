#!/usr/bin/env python3
"""
Script 267 - Drop 4 legacy molecular columns superseded by pinned single-source-of-truth feeders.

Targets on canonical_patient_master:
  * molecular_tested_v7      (contradicts molecular_tested_confirmed for 8,750 patients)
  * mol_test_count           (mismatches mol_n_tests for 8,834 patients; 9,585 NULLs)
  * molecular_platforms_v7   (superseded by mol_platform pin)
  * n_molecular_tests_v7     (stale; redundant with mol_n_tests)

Pinned feeders (set by Scripts 252-265) remain authoritative:
  * molecular_tested_confirmed, mol_has_thyroseq, mol_has_afirma, mol_platform
      <- canonical_molecular_tested_v1
  * mol_n_tests <- _molecular_patient_rollup_v227

Workflow:
  1. Pre-state audit (mismatch counts, value distributions) -> 267_pre.json
  2. Log each dropped column to manuscript_workspace.legacy_column_sweep_v1_1
  3. Drop via column-exclusion CREATE OR REPLACE TABLE
  4. Post-state assertions (row=10871, col=1491, none of the 4 names remain)
  5. Workspace view compile check (STOP if any view references a dropped col)
  6. Post-state report -> 267_post.json
"""
from __future__ import annotations

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

RUN_LOG = OUT_DIR / "267_run.log"
PRE_JSON = OUT_DIR / "267_pre.json"
POST_JSON = OUT_DIR / "267_post.json"

SCRIPT_TAG = "Script 267"
SCRIPT_NUM = "267"
RUN_DATE = "2026-04-17"

CPM = "canonical_patient_master"
LCS = f"{PUBLICATION_DB}.manuscript_workspace.legacy_column_sweep_v1_1"

LEGACY_COLS = [
    "molecular_tested_v7",
    "mol_test_count",
    "molecular_platforms_v7",
    "n_molecular_tests_v7",
]

# (column_name, version, stem, successor_column)
LEGACY_LOG_ROWS = [
    ("molecular_tested_v7", 7, "molecular_tested",
     "molecular_tested_confirmed (canonical_molecular_tested_v1) - "
     "v7 disagreed for 8,750 of 10,871 patients; dropped by Script 267 on 2026-04-17"),
    ("n_molecular_tests_v7", 7, "n_molecular_tests",
     "mol_n_tests (_molecular_patient_rollup_v227) - "
     "redundant; dropped by Script 267 on 2026-04-17"),
    ("molecular_platforms_v7", 7, "molecular_platforms",
     "mol_platform (canonical_molecular_tested_v1) - "
     "superseded by patient-level rollup; dropped by Script 267 on 2026-04-17"),
    ("mol_test_count", None, "mol_test_count",
     "mol_n_tests (_molecular_patient_rollup_v227) - "
     "mismatched mol_n_tests for 8,834 patients (9,585 NULLs); "
     "dropped by Script 267 on 2026-04-17"),
]


class TeeLogger:
    def __init__(self, path: Path) -> None:
        self.fh = path.open("w", encoding="utf-8")

    def __call__(self, msg: str = "") -> None:
        print(msg)
        self.fh.write(msg + "\n")
        self.fh.flush()

    def close(self) -> None:
        self.fh.close()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def pre_state_audit(con, log) -> dict:
    log("\n--- PRE-STATE AUDIT ---")
    out: dict = {
        "captured_at": utc_now(),
        "cpm_starting_rows": None,
        "cpm_starting_cols": None,
        "legacy_columns_present": [],
        "value_distributions": {},
        "contradictions": {},
    }

    n_rows = con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0]
    n_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
    """).fetchone()[0]
    out["cpm_starting_rows"] = int(n_rows)
    out["cpm_starting_cols"] = int(n_cols)
    log(f"  CPM starting state: {n_rows} rows x {n_cols} cols")

    if n_rows != 10871:
        raise SystemExit(f"PREFLIGHT FAIL: CPM rows={n_rows}, expected 10871. Aborting.")

    present_rows = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
          AND column_name IN ({','.join(repr(c) for c in LEGACY_COLS)})
        ORDER BY column_name
    """).fetchall()
    present = sorted(r[0] for r in present_rows)
    out["legacy_columns_present"] = present
    log(f"  Legacy columns present: {present}")
    if set(present) != set(LEGACY_COLS):
        missing = sorted(set(LEGACY_COLS) - set(present))
        log(f"  WARNING - Some legacy columns already absent: {missing}")

    for col in LEGACY_COLS:
        if col not in present:
            out["value_distributions"][col] = {"status": "ALREADY_ABSENT"}
            continue
        rows = con.execute(
            f"SELECT {col} AS v, COUNT(*) AS n FROM {CPM} GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall()
        nul = con.execute(
            f"SELECT COUNT(*) FROM {CPM} WHERE {col} IS NULL"
        ).fetchone()[0]
        dist = {"NULL": int(nul)}
        for v, n in rows:
            key = "NULL" if v is None else str(v)
            dist[key] = int(n) if key not in dist or dist[key] != int(n) else dist[key]
        out["value_distributions"][col] = dist
        sample_lines = [f"    {k}: {v}" for k, v in list(dist.items())[:6]]
        log(f"  {col} distribution (top values):")
        for line in sample_lines:
            log(line)

    if "molecular_tested_v7" in present:
        n = con.execute(f"""
            SELECT COUNT(*) FROM {CPM}
            WHERE COALESCE(molecular_tested_v7, FALSE)
              <> COALESCE(molecular_tested_confirmed, FALSE)
        """).fetchone()[0]
        out["contradictions"]["molecular_tested_v7_vs_molecular_tested_confirmed"] = int(n)
        log(f"  CONTRADICTION molecular_tested_v7 vs molecular_tested_confirmed: {n} patients")

    if "mol_test_count" in present:
        n_mismatch = con.execute(f"""
            SELECT COUNT(*) FROM {CPM}
            WHERE COALESCE(mol_test_count, -1) <> COALESCE(mol_n_tests, -1)
        """).fetchone()[0]
        n_null = con.execute(
            f"SELECT COUNT(*) FROM {CPM} WHERE mol_test_count IS NULL"
        ).fetchone()[0]
        out["contradictions"]["mol_test_count_vs_mol_n_tests_mismatch"] = int(n_mismatch)
        out["contradictions"]["mol_test_count_nulls"] = int(n_null)
        log(f"  CONTRADICTION mol_test_count vs mol_n_tests: {n_mismatch} mismatches, {n_null} nulls")

    if "n_molecular_tests_v7" in present:
        n = con.execute(f"""
            SELECT COUNT(*) FROM {CPM}
            WHERE COALESCE(n_molecular_tests_v7, -1) <> COALESCE(mol_n_tests, -1)
        """).fetchone()[0]
        out["contradictions"]["n_molecular_tests_v7_vs_mol_n_tests"] = int(n)
        log(f"  CONTRADICTION n_molecular_tests_v7 vs mol_n_tests: {n} patients")

    PRE_JSON.write_text(json.dumps(out, indent=2, default=str))
    log(f"  wrote {PRE_JSON}")
    return out


def log_to_legacy_sweep(con, log) -> int:
    log("\n--- LOG TO legacy_column_sweep_v1_1 ---")
    con.execute(f"DELETE FROM {LCS} WHERE column_name IN ({','.join(repr(c) for c in LEGACY_COLS)})")

    now_ts = con.execute("SELECT current_timestamp").fetchone()[0]
    rows = []
    for column_name, version, stem, succ in LEGACY_LOG_ROWS:
        rows.append((column_name, version, stem, None, succ, now_ts))

    con.executemany(
        f"""
        INSERT INTO {LCS}
          (column_name, version, stem, max_version_in_cpm, successor_column, inventoried_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    n = con.execute(
        f"SELECT COUNT(*) FROM {LCS} WHERE column_name IN ({','.join(repr(c) for c in LEGACY_COLS)})"
    ).fetchone()[0]
    log(f"  inserted {len(rows)} sweep rows; verified count = {n}")
    return int(n)


def drop_columns(con, log) -> tuple[int, int]:
    log("\n--- DROP COLUMNS via column-exclusion CTAS ---")
    excludes = ", ".join(LEGACY_COLS)
    con.execute(f"""
        CREATE OR REPLACE TABLE {CPM} AS
        SELECT * EXCLUDE ({excludes}) FROM {CPM}
    """)
    n_rows = con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0]
    n_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
    """).fetchone()[0]
    log(f"  Post-drop CPM: {n_rows} rows x {n_cols} cols")
    return int(n_rows), int(n_cols)


def post_state_assertions(con, log, n_rows: int, n_cols: int) -> None:
    log("\n--- POST-STATE ASSERTIONS ---")
    if n_rows != 10871:
        raise SystemExit(f"FAIL: CPM rows={n_rows}, expected 10871")
    log(f"  pass spine: rows={n_rows} == 10871")
    if n_cols != 1491:
        raise SystemExit(f"FAIL: CPM cols={n_cols}, expected 1491 (1495 - 4)")
    log(f"  pass cols: {n_cols} == 1491")
    remaining = con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
          AND column_name IN ({','.join(repr(c) for c in LEGACY_COLS)})
    """).fetchall()
    if remaining:
        raise SystemExit(f"FAIL: legacy cols still present: {[r[0] for r in remaining]}")
    log("  pass: none of the 4 legacy column names remain")


def view_compile_check(con, log) -> dict:
    log("\n--- WORKSPACE VIEW COMPILE CHECK ---")
    views = [r[0] for r in con.execute(f"""
        SELECT table_name FROM information_schema.views
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'
        ORDER BY table_name
    """).fetchall()]
    log(f"  workspace views to test: {len(views)}")

    failures: list[dict] = []
    legacy_cited_failures: list[dict] = []
    other_failures: list[dict] = []
    n_pass = 0

    for vn in views:
        try:
            con.execute(
                f'SELECT * FROM {PUBLICATION_DB}.manuscript_workspace."{vn}" LIMIT 1'
            ).fetchall()
            n_pass += 1
        except Exception as e:
            err = str(e)
            cited = [c for c in LEGACY_COLS if c in err]
            entry = {"view": vn, "error": err[:500], "legacy_cols_cited": cited}
            failures.append(entry)
            if cited:
                legacy_cited_failures.append(entry)
                log(f"  CRITICAL: {vn} references dropped column(s) {cited}")
            else:
                other_failures.append(entry)
                log(f"  WARN: {vn} fails for unrelated reason: {err[:160]}")

    log(f"  passed: {n_pass}/{len(views)}; failed: {len(failures)} "
        f"({len(legacy_cited_failures)} cite a dropped col)")

    if legacy_cited_failures:
        raise SystemExit(
            "STOP: workspace view(s) reference dropped legacy columns. "
            "Per prompt instructions, do NOT patch automatically. "
            f"Affected: {[f['view'] for f in legacy_cited_failures]}"
        )

    return {
        "n_views_checked": len(views),
        "n_pass": n_pass,
        "n_fail": len(failures),
        "legacy_cited_failures": legacy_cited_failures,
        "other_failures": other_failures,
    }


def main() -> int:
    log = TeeLogger(RUN_LOG)
    t0 = time.time()
    try:
        log("=" * 78)
        log(f"=== START {SCRIPT_TAG} - drop 4 legacy molecular columns")
        log(f"started_at: {utc_now()}")

        con = connect_locked()
        log(f"connected to {PUBLICATION_DB}")

        pre = pre_state_audit(con, log)

        present_set = set(pre["legacy_columns_present"])
        if not present_set:
            log("\nAll 4 legacy columns already absent; nothing to drop.")
            POST_JSON.write_text(json.dumps({
                "status": "NO_OP_ALL_ALREADY_ABSENT",
                "post_cpm_rows": pre["cpm_starting_rows"],
                "post_cpm_cols": pre["cpm_starting_cols"],
            }, indent=2))
            return 0
        if present_set != set(LEGACY_COLS):
            raise SystemExit(
                f"PARTIAL STATE: only {sorted(present_set)} present; "
                "expected all 4 or none. Aborting."
            )

        log_to_legacy_sweep(con, log)

        n_rows, n_cols = drop_columns(con, log)
        post_state_assertions(con, log, n_rows, n_cols)

        view_check = view_compile_check(con, log)

        post = {
            "captured_at": utc_now(),
            "post_cpm_rows": n_rows,
            "post_cpm_cols": n_cols,
            "removed_columns": LEGACY_COLS,
            "legacy_sweep_rows_inserted": len(LEGACY_LOG_ROWS),
            "view_compile_check": view_check,
            "elapsed_seconds": round(time.time() - t0, 1),
        }
        POST_JSON.write_text(json.dumps(post, indent=2, default=str))
        log(f"\nwrote {POST_JSON}")

        log(f"\n=== END {SCRIPT_TAG} elapsed={time.time() - t0:.1f}s")
        return 0

    except Exception as e:
        log(f"\nFATAL: {e!r}")
        import traceback
        log(traceback.format_exc())
        return 1
    finally:
        log.close()


if __name__ == "__main__":
    sys.exit(main())
