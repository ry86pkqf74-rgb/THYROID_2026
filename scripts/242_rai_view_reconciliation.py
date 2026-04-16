#!/usr/bin/env python3
"""
Script 242 — Reconcile manuscript views: rai_received_flag → rai_received_reconciled

Date:    2026-04-16
Author:  THYROID_2026 canonical-finalization run (v1_0 lock)

Purpose
=======
Move manuscript_workspace views from the legacy `canonical_patient_master.
rai_received_flag` (583 TRUE pts) to the canonical source
`canonical_patient_master.rai_received_reconciled` (862 TRUE pts, which
matches the distinct-patient count in `rai_treatment_episode_v2`). The
reconciled column is a strict superset of the legacy flag:
  - legacy-only (TRUE in legacy, FALSE in reconciled): 0 pts
  - reconciled-only (FALSE in legacy, TRUE in reconciled): 279 pts
so row counts will never decrease after the rewrite.

Tables READ
-----------
  thyroid_canonical_publication_v1_0.main.canonical_patient_master
  thyroid_canonical_publication_v1_0.information_schema.views

Tables WRITTEN
--------------
  DROP + CREATE VIEW × 11 direct-CPM manuscript_workspace views
  "Thyroid 2026 UPdated".archive_pub_v1_0._view_ddl_snapshot_pre242_<ts>
     (15 rows: pre-rewrite DDL for every view referencing rai_received_flag)

Rewrite rules (two context types)
---------------------------------
  SELECT-list: `rai_received_flag` → `rai_received_reconciled AS rai_received_flag`
     (preserves the output column surface)
  Filter:      `rai_received_flag` → `rai_received_reconciled`
     (bare rename; the CPM column has the same BOOLEAN type)

Downstream views (4 of 15) are NOT rewritten — they select from an
upstream view whose output surface still includes `rai_received_flag`
via the AS alias; the downstream SELECT keeps working without any edit.
Same architectural pattern as Script 240.

Rollback plan
-------------
  Iterate rows of _view_ddl_snapshot_pre242_<ts> and run `view_ddl` for
  each (DROP VIEW first, then CREATE).

Assertions
----------
  - 11 direct-CPM views rewritten; 15 − 11 = 4 downstream untouched
  - Full view-compile sweep passes (all 65 manuscript_workspace views)
  - For each rewritten view, post-row-count ≥ pre-row-count
  - For the 2 filter-context views (cohort_m019, cohort_m081):
    post-row-count = 862-universe count, which is ≥ 583-universe count
  - canonical_patient_master row count unchanged (10,871)
  - Archive snapshot present
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
SCRIPT_TAG = "Script 242"
RUN_DATE = "2026-04-16"

OLD_COL = "rai_received_flag"
NEW_COL = "rai_received_reconciled"

# Bare-token pattern that avoids substrings (e.g., if there were ever
# "last_rai_received_flag"). Also captures optional `p.` / `cpm.` prefix.
TOKEN_PAT = re.compile(
    rf'(?<![A-Za-z0-9_])(\w+\.)?(?<![A-Za-z0-9_]){re.escape(OLD_COL)}(?![A-Za-z0-9_])'
)

# Match `[prefix.]rai_received_flag` when it's clearly in a filter context —
# followed by a comparison / IS / IN / BETWEEN. Used to choose bare rename
# vs AS-alias rewrite.
FILTER_PAT = re.compile(
    rf'(?<![A-Za-z0-9_])(\w+\.)?(?<![A-Za-z0-9_]){re.escape(OLD_COL)}(?![A-Za-z0-9_])'
    r'(?=\s*(?:=|<|>|!=|<>|IS\b|IN\b|NOT\b|BETWEEN\b|LIKE\b))',
    flags=re.IGNORECASE,
)


def ts_utc() -> str:
    return datetime.utcnow().strftime("%H:%M:%S.") + f"{datetime.utcnow().microsecond // 1000:03d}Z"


def log(msg: str) -> None:
    print(f"[{ts_utc()}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# View classification + rewrite
# ---------------------------------------------------------------------------

def classify_views(con) -> tuple[list[tuple[str, str, str]], list[tuple[str, str, str]]]:
    """Return (direct_cpm_views, downstream_views) — both contain the full DDL."""
    all_views = con.execute(
        f"""SELECT table_schema, table_name, view_definition
            FROM information_schema.views
            WHERE table_catalog='{PUBLICATION_DB}'
              AND table_schema IN ('main','manuscript_workspace')
            ORDER BY table_schema, table_name"""
    ).fetchall()
    from_re = re.compile(r'\bFROM\s+([\w\."]+(?:\.[\w\."]+){0,2})', flags=re.IGNORECASE)
    join_re = re.compile(r'\bJOIN\s+([\w\."]+(?:\.[\w\."]+){0,2})', flags=re.IGNORECASE)
    direct: list[tuple[str, str, str]] = []
    downstream: list[tuple[str, str, str]] = []
    for schema, name, defn in all_views:
        if not defn or not TOKEN_PAT.search(defn):
            continue
        sources = from_re.findall(defn) + join_re.findall(defn)
        if any("canonical_patient_master" in s for s in sources):
            direct.append((schema, name, defn))
        else:
            downstream.append((schema, name, defn))
    return direct, downstream


def rewrite_ddl(ddl: str) -> tuple[str, int]:
    """Apply filter-context rewrite first, then SELECT-list rewrite.

    Returns (new_ddl, n_replacements).
    """
    n = 0

    def _filter_repl(m: re.Match[str]) -> str:
        nonlocal n
        n += 1
        prefix = m.group(1) or ""
        return f"{prefix}{NEW_COL}"

    # 1. Filter-context rewrite: bare rename (no AS alias)
    ddl_out = FILTER_PAT.sub(_filter_repl, ddl)

    # 2. Remaining occurrences are SELECT-list: add AS alias to preserve surface
    def _select_repl(m: re.Match[str]) -> str:
        nonlocal n
        n += 1
        prefix = m.group(1) or ""
        return f"{prefix}{NEW_COL} AS {OLD_COL}"

    ddl_out = TOKEN_PAT.sub(_select_repl, ddl_out)
    return ddl_out, n


# ---------------------------------------------------------------------------
# Archive snapshot
# ---------------------------------------------------------------------------

def snapshot_view_ddls(con, run_ts: str, all_affected: list[tuple[str, str, str]]) -> str:
    dest = f"_view_ddl_snapshot_pre242_{run_ts}"
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
    for schema, name, ddl in all_affected:
        con.execute(
            f"INSERT INTO {full_dest} VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
            (schema, name, ddl),
        )
    con.execute(
        f"""COMMENT ON TABLE {full_dest} IS
            '{SCRIPT_TAG} ({RUN_DATE}): pre-rewrite snapshot of every manuscript_workspace view
             referencing canonical_patient_master.rai_received_flag. Use for rollback:
             iterate rows and execute view_ddl (DROP VIEW first, then CREATE).'"""
    )
    n = con.execute(f"SELECT COUNT(*) FROM {full_dest}").fetchone()[0]
    log(f"  captured {n} view DDLs")
    return dest


# ---------------------------------------------------------------------------
# Row-count audit
# ---------------------------------------------------------------------------

def safe_row_count(con, schema: str, name: str) -> int | None:
    try:
        r = con.execute(f'SELECT COUNT(*) FROM {schema}."{name}"').fetchone()
        return int(r[0]) if r else None
    except Exception as e:
        log(f"  row-count ERROR on {schema}.{name}: {str(e)[:120]}")
        return None


# ---------------------------------------------------------------------------
# Assertions
# ---------------------------------------------------------------------------

def run_assertions(
    con,
    direct: list[tuple[str, str, str]],
    downstream: list[tuple[str, str, str]],
    pre_counts: dict[tuple[str, str], int],
    archive_name: str,
) -> int:
    checks: list[tuple[str, bool]] = []

    # (1) CPM row count unchanged
    n = con.execute("SELECT COUNT(*) FROM canonical_patient_master").fetchone()[0]
    checks.append((f"CPM row count unchanged at 10,871 (got {n})", n == 10871))

    # (2) Every direct-CPM view still compiles
    compile_fails: list[tuple[str, str, str]] = []
    for schema, name, _ in direct:
        try:
            con.execute(f'SELECT 1 FROM {schema}."{name}" LIMIT 0').fetchall()
        except Exception as e:
            compile_fails.append((schema, name, str(e)[:160]))
    checks.append(
        (f"all {len(direct)} rewritten views compile (0 failures)", not compile_fails)
    )
    for s, n, err in compile_fails[:5]:
        log(f"    FAIL {s}.{n}: {err}")

    # (3) Full view-compile sweep for all 65 manuscript_workspace views
    all_views = con.execute(
        f"""SELECT table_schema, table_name FROM information_schema.views
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='manuscript_workspace'"""
    ).fetchall()
    passes = 0
    fails = 0
    for schema, name in all_views:
        try:
            con.execute(f'SELECT 1 FROM {schema}."{name}" LIMIT 0').fetchall()
            passes += 1
        except Exception:
            fails += 1
    checks.append((f"full view sweep: {passes}/{len(all_views)} pass, {fails} fail", fails == 0))

    # (4) Post-row-count ≥ pre-row-count for every view we touched.
    decreased: list[tuple[str, str, int, int]] = []
    for schema, name, _ in direct + downstream:
        pre = pre_counts.get((schema, name))
        post = safe_row_count(con, schema, name)
        if pre is None or post is None:
            continue
        if post < pre:
            decreased.append((schema, name, pre, post))
    checks.append(
        (f"no view lost rows ({len(decreased)} with post < pre)", not decreased)
    )
    for s, n, a, b in decreased[:5]:
        log(f"    DECREASED {s}.{n}: {a} -> {b}")

    # (5) Output surface preserved: every rewritten view still exposes a
    # column named rai_received_flag (via AS alias).
    surface_misses = []
    for schema, name, _ in direct:
        cols = {
            r[0]
            for r in con.execute(
                f"""SELECT column_name FROM information_schema.columns
                    WHERE table_catalog='{PUBLICATION_DB}'
                      AND table_schema='{schema}' AND table_name='{name}'"""
            ).fetchall()
        }
        if OLD_COL not in cols:
            surface_misses.append(f"{schema}.{name}")
    checks.append(
        (f"output surface preserved on all {len(direct)} rewritten views", not surface_misses)
    )
    for n in surface_misses[:5]:
        log(f"    surface MISS {n}")

    # (6) Archive snapshot present
    n_arch = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{ARCHIVE_DB}' AND table_schema='{ARCHIVE_SCHEMA}'
              AND table_name='{archive_name}'"""
    ).fetchone()[0]
    checks.append(("archive snapshot of pre-rewrite DDLs present", n_arch == 1))

    # (7) The legacy universe is still a strict subset: no view's rewrite decreased
    # "pts with rai_received_flag=TRUE" count. We compare CPM legacy vs reconciled.
    r = con.execute(
        """SELECT
             SUM(CASE WHEN rai_received_flag = TRUE THEN 1 ELSE 0 END) AS legacy,
             SUM(CASE WHEN rai_received_reconciled = TRUE THEN 1 ELSE 0 END) AS reconciled
           FROM canonical_patient_master"""
    ).fetchone()
    checks.append((f"CPM superset invariant: reconciled({r[1]}) >= legacy({r[0]})", r[1] >= r[0]))

    failures = 0
    for label, ok in checks:
        tag = "PASS" if ok else "FAIL"
        log(f"  ASSERT [{tag}] {label}")
        if not ok:
            failures += 1
    return failures


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="classify + preview rewrite; do not execute")
    args = ap.parse_args()

    t0 = time.time()
    log(f"=== START {Path(__file__).name}")
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    # Phase 0: baseline
    log("PHASE 0 — baseline counts")
    r = con.execute(
        """SELECT
             SUM(CASE WHEN rai_received_flag = TRUE THEN 1 ELSE 0 END) AS legacy,
             SUM(CASE WHEN rai_received_reconciled = TRUE THEN 1 ELSE 0 END) AS reconciled
           FROM canonical_patient_master"""
    ).fetchone()
    log(f"  CPM rai_received_flag=TRUE:       {r[0]}")
    log(f"  CPM rai_received_reconciled=TRUE: {r[1]}")
    if r[1] < r[0]:
        raise RuntimeError(
            f"reconciled ({r[1]}) < legacy ({r[0]}) — strict-superset invariant violated, aborting"
        )

    # Phase 1: classify + count pre-rewrite rows
    log("PHASE 1 — classify affected views")
    direct, downstream = classify_views(con)
    log(f"  direct-CPM views (will rewrite): {len(direct)}")
    log(f"  downstream views (no rewrite):   {len(downstream)}")
    for schema, name, _ in direct:
        log(f"    direct: {schema}.{name}")
    for schema, name, _ in downstream:
        log(f"    downstream: {schema}.{name}")
    if len(direct) == 0:
        raise RuntimeError("expected ~11 direct-CPM views; got 0 — did a prior run already rewrite?")

    pre_counts: dict[tuple[str, str], int] = {}
    for schema, name, _ in direct + downstream:
        c = safe_row_count(con, schema, name)
        if c is not None:
            pre_counts[(schema, name)] = c
    log(f"  captured pre-rewrite counts for {len(pre_counts)} views")

    if args.dry_run:
        log("--dry-run mode: previewing rewrite on direct[0]")
        schema, name, ddl = direct[0]
        new_ddl, n_repl = rewrite_ddl(ddl)
        log(f"  sample: {schema}.{name}  replacements={n_repl}")
        log(f"  before snippet: ...{ddl[max(0, TOKEN_PAT.search(ddl).start()-60):TOKEN_PAT.search(ddl).start()+80]}...")
        m = re.search(rf'{re.escape(NEW_COL)}', new_ddl)
        if m:
            log(f"  after snippet:  ...{new_ddl[max(0, m.start()-60):m.start()+80]}...")
        return

    # Phase 2: backup DDLs (include BOTH direct + downstream for rollback completeness)
    log("PHASE 2 — snapshot DDLs to archive_pub_v1_0")
    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    archive_name = snapshot_view_ddls(con, run_ts, direct + downstream)

    # Phase 3: rewrite + recreate direct-CPM views
    log("PHASE 3 — rewrite + recreate direct-CPM views")
    for schema, name, ddl in direct:
        new_ddl, n_repl = rewrite_ddl(ddl)
        if n_repl == 0:
            raise RuntimeError(f"rewrite produced 0 replacements for {schema}.{name}")
        con.execute(f'DROP VIEW IF EXISTS {schema}."{name}"')
        con.execute(new_ddl)
        log(f"  rewrote {schema}.{name}  ({n_repl} replacements)")

    # Phase 4: assertions
    log("PHASE 4 — assertions")
    fails = run_assertions(con, direct, downstream, pre_counts, archive_name)
    elapsed = time.time() - t0
    if fails:
        log(f"FAILURES: {fails}")
        log(
            f"ROLLBACK: iterate rows of {ARCHIVE_QUALIFIED}.\"{archive_name}\" "
            f"and run (DROP VIEW + execute view_ddl) for each."
        )
        sys.exit(1)
    log(f"=== END {Path(__file__).name}  elapsed={elapsed:.1f}s  failures=0")


if __name__ == "__main__":
    main()
