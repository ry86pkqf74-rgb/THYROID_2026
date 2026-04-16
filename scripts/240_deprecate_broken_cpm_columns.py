#!/usr/bin/env python3
"""
Script 240 — Deprecate broken CPM columns (rename + view rewrite sweep)

Date:    2026-04-16
Author:  THYROID_2026 canonical-finalization run (v1_0 lock)

Purpose
=======
Rename two broken `canonical_patient_master` columns to the `deprecated__`
namespace (without dropping them — v1_1 will do that) and rewrite every
downstream view that references them so the output column surface stays
stable. Adds COMMENT markers explaining the deprecation.

Rename targets
--------------
  1. imaging_nodule_size_cm  →  deprecated__imaging_nodule_size_cm
       (Broken per-patient aggregation: 44.8% MAX / 31.5% MIN / 15.1% MEAN
        across 3,439 patients. Superseded by dominant_nodule_size_cm.)
  2. tumor_size_cm           →  deprecated__tumor_size_cm
       (Byte-identical duplicate of path_tumor_size_cm, 4130/4130
        populated rows. Superseded by path_tumor_size_cm.)

Pre-flight ground truth (Script 240 pre-flight, 2026-04-16)
-----------------------------------------------------------
  - 55 views reference tumor_size_cm (40 bare token, 15 with "p." prefix)
  - 1 view references imaging_nodule_size_cm (with "p." prefix)
  - Overlap: 1 view (cohort_m025_tirads_performance_v1) references both.
  - ALL references occur in SELECT-list context — zero in
    WHERE / GROUP BY / ORDER BY. Confirmed by context-classifying regex
    across all 65 manuscript_workspace views.
  - DuckDB does NOT auto-alias renamed columns (verified in scratch
    table + view test). Rewrite is mandatory, not optional.
  - 55 total unique affected views (the imaging-nodule view is a subset
    of the tumor views: cohort_m025_tirads_performance_v1).
  - FROM-source split:
      * 32 views directly reference canonical_patient_master and therefore
        REQUIRE rewrite (replace column reference with
        `deprecated__X AS X` so output surface stays stable).
      * 23 views are downstream (FROM another manuscript_workspace view).
        These DO NOT require rewrite — their upstream base view's
        `AS X` alias re-exposes the original column name, so the
        downstream SELECT of `tumor_size_cm` keeps working without any
        edit. They are compile-tested in the post-rewrite sweep.

Tables READ
-----------
  thyroid_canonical_publication_v1_0.main.canonical_patient_master
  thyroid_canonical_publication_v1_0.information_schema.views
  thyroid_canonical_publication_v1_0.information_schema.columns

Tables WRITTEN
--------------
  ALTER TABLE  canonical_patient_master (RENAME COLUMN × 2)
  COMMENT ON COLUMN × 2
  DROP VIEW   manuscript_workspace.cohort_*   (55 views)
  CREATE VIEW manuscript_workspace.cohort_*   (55 views, with rewritten DDL)
  Archive:
    "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre240_v2_backup_<ts>
    "Thyroid 2026 UPdated".archive_pub_v1_0._view_ddl_snapshot_pre240_<ts>
      (audit snapshot of the pre-rewrite view definitions for all 55 views)

Rollback plan
-------------
  1. Restore CPM column names:
        ALTER TABLE canonical_patient_master RENAME COLUMN deprecated__tumor_size_cm TO tumor_size_cm;
        ALTER TABLE canonical_patient_master RENAME COLUMN deprecated__imaging_nodule_size_cm TO imaging_nodule_size_cm;
  2. Restore views from archive_pub_v1_0._view_ddl_snapshot_pre240_<ts>:
     iterate each row and run the captured CREATE VIEW DDL.
  3. The per-row CPM values stay identical (RENAME COLUMN is a metadata op).

Assertion block (runs at end; exits non-zero on any FAIL)
---------------------------------------------------------
  a. Both renames succeeded (new names present, old names absent)
  b. COMMENT ON COLUMN set for both deprecated columns
  c. CPM row count unchanged at 10,871
  d. CPM column count unchanged
  e. All 65 manuscript_workspace views compile (SELECT 1 FROM v LIMIT 0)
  f. Every rewritten view exposes output column names that match its
     pre-rewrite surface (tumor_size_cm / imaging_nodule_size_cm still
     in the view's column list)
  g. Archive copy of CPM present; archive copy of view DDL snapshot present.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_QUALIFIED = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
SCRIPT_TAG = "Script 240"
RUN_DATE = "2026-04-16"

# The two columns we deprecate in this script.
DEPRECATIONS: dict[str, dict[str, str]] = {
    "imaging_nodule_size_cm": {
        "new_name": "deprecated__imaging_nodule_size_cm",
        "replacement": "dominant_nodule_size_cm",
        "reason": (
            "inconsistent per-patient aggregation (MAX/MIN/MEAN mixed) — "
            "44.8% MAX, 31.5% MIN, 15.1% MEAN across 3,439 patients"
        ),
    },
    "tumor_size_cm": {
        "new_name": "deprecated__tumor_size_cm",
        "replacement": "path_tumor_size_cm",
        "reason": "byte-identical duplicate of path_tumor_size_cm across 4130/4130 populated rows",
    },
}


def ts_utc() -> str:
    return datetime.utcnow().strftime("%H:%M:%S.") + f"{datetime.utcnow().microsecond // 1000:03d}Z"


def log(msg: str) -> None:
    print(f"[{ts_utc()}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# View DDL rewriter — regex-based, SELECT-list-context safe
# ---------------------------------------------------------------------------

def build_rewriter(old: str, new: str) -> re.Pattern[str]:
    """Build a compiled regex for one column rename.

    Matches an optional `table.` or `alias.` prefix, then the bare column
    name (not followed by _ / letter / digit to avoid `path_tumor_size_cm`
    false positives; not preceded by _ or alphanumeric for the same
    reason).  Replacement adds ` AS {old}` to keep the output surface.

    Safe only when the match is in a SELECT-list context (verified by
    pre-flight for all 55 target views).
    """
    pattern = re.compile(
        rf'(?<![A-Za-z0-9_])(\w+\.)?(?<![A-Za-z0-9_]){re.escape(old)}(?![A-Za-z0-9_])',
    )
    return pattern


def rewrite_ddl(ddl: str) -> tuple[str, int]:
    """Apply both column rewrites. Return (new_ddl, n_replacements).

    The replacement for each occurrence is:
        [optional_prefix] deprecated__{col} AS {col}
    so downstream view consumers still see the original name.
    """
    n = 0

    def _repl_builder(old: str, new: str):
        def _repl(m: re.Match[str]) -> str:
            nonlocal n
            n += 1
            prefix = m.group(1) or ""
            return f"{prefix}{new} AS {old}"

        return _repl

    ddl_out = ddl
    for old, spec in DEPRECATIONS.items():
        ddl_out = build_rewriter(old, spec["new_name"]).sub(
            _repl_builder(old, spec["new_name"]), ddl_out
        )
    return ddl_out, n


# ---------------------------------------------------------------------------
# Phase helpers
# ---------------------------------------------------------------------------

def capture_baseline(con) -> dict:
    """Snapshot baseline state before any writes."""
    baseline: dict[str, object] = {}
    baseline["cpm_rows"] = con.execute(
        "SELECT COUNT(*) FROM canonical_patient_master"
    ).fetchone()[0]
    baseline["cpm_cols"] = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='canonical_patient_master'"""
    ).fetchone()[0]
    # Column presence
    for old, spec in DEPRECATIONS.items():
        for name in (old, spec["new_name"]):
            present = con.execute(
                f"""SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                      AND table_name='canonical_patient_master' AND column_name='{name}'"""
            ).fetchone()[0]
            baseline[f"col_present:{name}"] = bool(present)
    return baseline


def archive_cpm(con, run_ts: str) -> str:
    dest = f"canonical_patient_master_pre240_v2_backup_{run_ts}"
    full_dest = f'{ARCHIVE_QUALIFIED}."{dest}"'
    log(f"archive CPM -> {full_dest}")
    src_rc = con.execute("SELECT COUNT(*) FROM canonical_patient_master").fetchone()[0]
    con.execute(f"CREATE OR REPLACE TABLE {full_dest} AS SELECT * FROM canonical_patient_master")
    dst_rc = con.execute(f"SELECT COUNT(*) FROM {full_dest}").fetchone()[0]
    if src_rc != dst_rc:
        raise RuntimeError(f"CPM archive row mismatch src={src_rc} dst={dst_rc}")
    con.execute(
        f"""COMMENT ON TABLE {full_dest} IS
            '{SCRIPT_TAG} ({RUN_DATE}): pre-rename snapshot of canonical_patient_master.
            Captured before renaming tumor_size_cm and imaging_nodule_size_cm to
            their deprecated__* names. Use for rollback via ALTER TABLE RENAME COLUMN
            (metadata op, no data movement needed).'"""
    )
    log(f"  archived {src_rc} rows -> {dest}")
    return dest


def snapshot_view_ddls(con, run_ts: str, affected: list[tuple[str, str, str]]) -> str:
    """Persist the current CREATE VIEW DDL for every affected view.

    Writes to archive_pub_v1_0._view_ddl_snapshot_pre240_<ts> for rollback.
    """
    dest = f"_view_ddl_snapshot_pre240_{run_ts}"
    full_dest = f'{ARCHIVE_QUALIFIED}."{dest}"'
    log(f"snapshot view DDLs -> {full_dest}")
    con.execute(
        f"""CREATE OR REPLACE TABLE {full_dest} (
              schema_name VARCHAR,
              view_name   VARCHAR,
              view_ddl    VARCHAR,
              captured_at TIMESTAMP
            )"""
    )
    for schema, name, ddl in affected:
        # Use parameter binding to avoid escape issues in long DDLs.
        con.execute(
            f'INSERT INTO {full_dest} VALUES (?, ?, ?, CURRENT_TIMESTAMP)',
            (schema, name, ddl),
        )
    con.execute(
        f"""COMMENT ON TABLE {full_dest} IS
            '{SCRIPT_TAG} ({RUN_DATE}): pre-rewrite snapshot of every manuscript_workspace
             view referencing tumor_size_cm or imaging_nodule_size_cm. Use for rollback: for
             each row, execute view_ddl after reverting the CPM column renames.'"""
    )
    n = con.execute(f"SELECT COUNT(*) FROM {full_dest}").fetchone()[0]
    log(f"  captured {n} view DDLs")
    return dest


def enumerate_affected_views(con) -> tuple[list[tuple[str, str, str]], list[tuple[str, str, str]]]:
    """Return (direct_cpm_views, downstream_views).

    Only views whose FROM/JOIN clause references canonical_patient_master
    need column-name rewriting — they're the only ones whose binder will
    see the renamed `deprecated__*` columns.

    Views whose FROM/JOIN references another view do NOT need rewriting:
    the upstream view re-exposes the original name via the `AS X` alias,
    so their SELECT of the bare `X` still binds.
    """
    all_views = con.execute(
        f"""SELECT table_schema, table_name, view_definition
            FROM information_schema.views
            WHERE table_catalog='{PUBLICATION_DB}'
              AND table_schema IN ('main', 'manuscript_workspace')
            ORDER BY table_schema, table_name"""
    ).fetchall()
    patterns = [build_rewriter(old, spec["new_name"]) for old, spec in DEPRECATIONS.items()]
    direct: list[tuple[str, str, str]] = []
    downstream: list[tuple[str, str, str]] = []
    from_re = re.compile(r'\bFROM\s+([\w\."]+(?:\.[\w\."]+){0,2})', flags=re.IGNORECASE)
    join_re = re.compile(r'\bJOIN\s+([\w\."]+(?:\.[\w\."]+){0,2})', flags=re.IGNORECASE)
    for schema, name, defn in all_views:
        if not defn:
            continue
        if not any(p.search(defn) for p in patterns):
            continue
        sources = from_re.findall(defn) + join_re.findall(defn)
        refs_cpm = any("canonical_patient_master" in s for s in sources)
        if refs_cpm:
            direct.append((schema, name, defn))
        else:
            downstream.append((schema, name, defn))
    return direct, downstream


def apply_renames(con) -> None:
    for old, spec in DEPRECATIONS.items():
        new = spec["new_name"]
        log(f"ALTER TABLE canonical_patient_master RENAME {old} -> {new}")
        con.execute(
            f'ALTER TABLE canonical_patient_master '
            f'RENAME COLUMN "{old}" TO "{new}"'
        )
        comment = (
            f'DEPRECATED {RUN_DATE} ({SCRIPT_TAG}): {spec["reason"]}. '
            f'Superseded by {spec["replacement"]}. Will be removed in v1_1.'
        )
        con.execute(
            f'COMMENT ON COLUMN canonical_patient_master."{new}" IS '
            f"'{comment.replace(chr(39), chr(39)+chr(39))}'"
        )


def rewrite_and_replace_views(con, affected: list[tuple[str, str, str]]) -> list[tuple[str, str, int]]:
    """Rewrite + recreate each affected view; return [(schema, name, n_replacements)]."""
    results: list[tuple[str, str, int]] = []
    for schema, name, ddl in affected:
        new_ddl, n_repl = rewrite_ddl(ddl)
        if n_repl == 0:
            raise RuntimeError(f"rewrite produced 0 replacements for {schema}.{name} — regex miss?")
        # The captured DDL is a full CREATE VIEW ... statement; drop + execute.
        log(f"  rewrite {schema}.{name}  ({n_repl} replacements)")
        con.execute(f'DROP VIEW IF EXISTS {schema}."{name}"')
        con.execute(new_ddl)
        # Compile test
        con.execute(f'SELECT 1 FROM {schema}."{name}" LIMIT 0').fetchall()
        results.append((schema, name, n_repl))
    return results


def view_compile_sweep(con) -> tuple[int, int, list[tuple[str, str, str]]]:
    views = con.execute(
        f"""SELECT table_schema, table_name FROM information_schema.views
            WHERE table_catalog='{PUBLICATION_DB}'
              AND table_schema IN ('main', 'manuscript_workspace')
            ORDER BY table_schema, table_name"""
    ).fetchall()
    passes = 0
    failures: list[tuple[str, str, str]] = []
    for schema, name in views:
        try:
            con.execute(f'SELECT 1 FROM {schema}."{name}" LIMIT 0').fetchall()
            passes += 1
        except Exception as e:
            failures.append((schema, name, str(e)[:160]))
    return len(views), passes, failures


def check_view_surface(con, affected: list[tuple[str, str, str]]) -> list[tuple[str, str, list[str]]]:
    """For every rewritten view, confirm the original column name is still present."""
    misses: list[tuple[str, str, list[str]]] = []
    for schema, name, _ in affected:
        cols = {
            r[0]
            for r in con.execute(
                f"""SELECT column_name FROM information_schema.columns
                    WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='{schema}'
                      AND table_name='{name}'"""
            ).fetchall()
        }
        expected = []
        for old in DEPRECATIONS:
            # Only require surface-preservation for columns this view originally referenced.
            # Quick check: if the snapshotted DDL (pre-rewrite) mentioned the old name,
            # expect it to still be in the view's columns now.
            pre_ddl = next(d for s, n, d in affected if s == schema and n == name)
            if build_rewriter(old, DEPRECATIONS[old]["new_name"]).search(pre_ddl):
                expected.append(old)
        missing = [c for c in expected if c not in cols]
        if missing:
            misses.append((schema, name, missing))
    return misses


def run_assertions(
    con,
    baseline: dict,
    affected: list[tuple[str, str, str]],
    archive_cpm_name: str,
    archive_view_snapshot_name: str,
) -> int:
    checks: list[tuple[str, bool]] = []

    # (a) Both renames succeeded
    for old, spec in DEPRECATIONS.items():
        new = spec["new_name"]
        new_present = bool(
            con.execute(
                f"""SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                      AND table_name='canonical_patient_master' AND column_name='{new}'"""
            ).fetchone()[0]
        )
        old_absent = not bool(
            con.execute(
                f"""SELECT COUNT(*) FROM information_schema.columns
                    WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                      AND table_name='canonical_patient_master' AND column_name='{old}'"""
            ).fetchone()[0]
        )
        checks.append((f"CPM column '{new}' present",              new_present))
        checks.append((f"CPM column '{old}' absent (renamed away)", old_absent))

    # (b) COMMENT ON COLUMN set
    for old, spec in DEPRECATIONS.items():
        new = spec["new_name"]
        c = con.execute(
            f"""SELECT comment FROM duckdb_columns()
                WHERE database_name='{PUBLICATION_DB}' AND schema_name='main'
                  AND table_name='canonical_patient_master' AND column_name='{new}'"""
        ).fetchone()
        has_dep_marker = bool(c and c[0] and c[0].startswith("DEPRECATED 2026-04-16"))
        checks.append((f"COMMENT on {new} carries DEPRECATED marker", has_dep_marker))

    # (c) CPM row count unchanged
    n = con.execute("SELECT COUNT(*) FROM canonical_patient_master").fetchone()[0]
    checks.append((f"CPM row count unchanged at {baseline['cpm_rows']}", n == baseline["cpm_rows"]))

    # (d) CPM column count unchanged (renames don't add/remove columns)
    n_cols = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='canonical_patient_master'"""
    ).fetchone()[0]
    checks.append((f"CPM column count unchanged at {baseline['cpm_cols']}", n_cols == baseline["cpm_cols"]))

    # (e) All manuscript_workspace views compile
    total, passes, failures = view_compile_sweep(con)
    log(f"view-compile sweep: {passes}/{total} pass, {len(failures)} fail")
    for schema, name, err in failures[:10]:
        log(f"  FAIL {schema}.{name}: {err}")
    checks.append((f"view-compile sweep: all {total} views pass", passes == total))

    # (f) Output column surface preserved on every affected view
    misses = check_view_surface(con, affected)
    if misses:
        for schema, name, missing in misses[:10]:
            log(f"  surface miss {schema}.{name}: expected cols missing {missing}")
    checks.append((f"all {len(affected)} rewritten views preserve original column names", not misses))

    # (g) Archive entries present
    arch_cpm_n = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{ARCHIVE_DB}' AND table_schema='{ARCHIVE_SCHEMA}'
              AND table_name='{archive_cpm_name}'"""
    ).fetchone()[0]
    checks.append(("archive copy of pre-rename CPM present", arch_cpm_n == 1))
    arch_vds_n = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{ARCHIVE_DB}' AND table_schema='{ARCHIVE_SCHEMA}'
              AND table_name='{archive_view_snapshot_name}'"""
    ).fetchone()[0]
    checks.append(("archive copy of pre-rewrite view DDLs present", arch_vds_n == 1))

    failures_count = 0
    for label, ok in checks:
        tag = "PASS" if ok else "FAIL"
        log(f"  ASSERT [{tag}] {label}")
        if not ok:
            failures_count += 1
    return failures_count


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Enumerate + rewrite DDLs in memory but DO NOT execute any ALTER / DROP / CREATE.",
    )
    args = ap.parse_args()

    t0 = time.time()
    log(f"=== START {Path(__file__).name}")
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    # --- Phase 0: baseline capture ---------------------------------------
    log("PHASE 0 — baseline capture")
    baseline = capture_baseline(con)
    for k, v in baseline.items():
        log(f"  {k}: {v}")
    assert baseline["col_present:imaging_nodule_size_cm"], "imaging_nodule_size_cm missing — did a prior run rename it already?"
    assert baseline["col_present:tumor_size_cm"], "tumor_size_cm missing — did a prior run rename it already?"
    assert not baseline["col_present:deprecated__imaging_nodule_size_cm"], "deprecated__imaging_nodule_size_cm already exists"
    assert not baseline["col_present:deprecated__tumor_size_cm"], "deprecated__tumor_size_cm already exists"

    # --- Phase 1: enumerate affected views --------------------------------
    log("PHASE 1 — enumerate affected views (split by FROM source)")
    direct, downstream = enumerate_affected_views(con)
    log(f"  direct-CPM views (need rewrite):   {len(direct)}")
    log(f"  downstream views (no rewrite):     {len(downstream)}")
    for schema, name, _ in direct[:5]:
        log(f"    direct: {schema}.{name}")
    if len(direct) == 0:
        raise RuntimeError("pre-flight found ~32 direct-CPM views; got 0 — did a prior run already rewrite them?")

    # The archive snapshot captures BOTH direct and downstream views so the rollback
    # hint is complete (even though only direct views are rewritten).
    affected = direct + downstream

    if args.dry_run:
        log("--dry-run mode: previewing rewrite on one direct-CPM view")
        schema, name, ddl = direct[0]
        new_ddl, n_repl = rewrite_ddl(ddl)
        log(f"  sample: {schema}.{name}  replacements={n_repl}")
        log(f"  NEW DDL (first 400 chars):\n{new_ddl[:400]}")
        return

    # --- Phase 2: backups (CPM + view DDLs) -------------------------------
    log("PHASE 2 — backups")
    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    cpm_archive = archive_cpm(con, run_ts)
    view_snapshot = snapshot_view_ddls(con, run_ts, affected)

    # --- Phase 3: rename columns + add COMMENTs ---------------------------
    log("PHASE 3 — rename CPM columns + add DEPRECATED comments")
    apply_renames(con)

    # --- Phase 4: rewrite + replace ONLY the direct-CPM views -------------
    log("PHASE 4 — rewrite + recreate direct-CPM views")
    results = rewrite_and_replace_views(con, direct)
    log(f"  rewritten: {len(results)} direct-CPM views (downstream views intentionally untouched)")

    # --- Phase 5: assertions ---------------------------------------------
    log("PHASE 5 — assertions")
    n_fail = run_assertions(con, baseline, affected, cpm_archive, view_snapshot)
    elapsed = time.time() - t0
    if n_fail:
        log(f"FAILURES: {n_fail}")
        log(
            f"ROLLBACK: restore CPM column names with ALTER TABLE "
            f'"{PUBLICATION_DB}".main.canonical_patient_master RENAME COLUMN ... (metadata op), '
            f'then iterate rows of {ARCHIVE_QUALIFIED}."{view_snapshot}" executing view_ddl for each.'
        )
        sys.exit(1)
    log(f"=== END {Path(__file__).name}  elapsed={elapsed:.1f}s  failures=0")


if __name__ == "__main__":
    main()
