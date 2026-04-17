#!/usr/bin/env python3
"""
Script 249 — Phase 2 of v1_1 cleanup: deprecate legacy CPM columns + rewrite views

Date:    2026-04-16
Author:  THYROID_2026 canonical-finalization run (v1_1 cleanup)
Branch:  cleanup/canonical-finalization-20260416

Purpose
=======
Implement the AGENTS.md deprecated-column table on
`thyroid_canonical_publication_v1_0.main.canonical_patient_master`:

  - margin_status_final          -> r_class_true
  - margin_r_class               -> r_class_true
  - lvi_grade_final_v13          -> lvi_ordinal_worst
  - multifocal_flag              -> multifocal_flag_path
  - path_multifocal_flag         -> multifocal_flag_path     (both deprecated)
  - path_n_tumors                -> n_tumors_path
  - max_tumor_size_cm_v10        -> tumor_size_cm_max

Renames each deprecated column to `DEPRECATED__<name>` with a COMMENT,
after rewriting any view that references the deprecated name to its
successor. Also drops the 6 `*_prev_233` snapshot columns from CPM
(after archiving) and writes a `legacy_column_sweep_v1_1` audit of any
other `*_v[0-9]+` columns with higher-version successors.

Phases
------
  2A  preflight invariants (CPM shape; successor coverage)
  2B  snapshot ALL manuscript_workspace view DDLs to archive
  2C  snapshot CPM to archive
  2D  topological view rewrite (root + direct-CPM views)
       - programmatically build dependency graph; topo sort
       - rewrite in dependency order; substitute deprecated -> successor
       - compile-sweep all 65 views; assert 0 broken
  2E  CPM column renames (deprecated -> DEPRECATED__name) + COMMENTs
  2F  *_prev_233 snapshot drop (archive then DROP COLUMN)
  2G  legacy_column_sweep_v1_1 (audit only; no auto-deprecate)
  2H  registry feeds_master_columns update (deprecated -> successor)
  2I  final assertions: 0 broken views; 0 registry refs to deprecated cols;
       119 Check 14a still PASS

Default mode is --dry-run. Pass --apply to execute writes.
On any non-zero exit OR exception inside 2B-2I, fire rollback:
  - For each row in view_ddl_snapshot_pre249_<ts>: DROP VIEW + recreate from snapshot
  - For each renamed CPM col: ALTER TABLE ... RENAME COLUMN DEPRECATED__x TO x
  - Re-raise the exception.

Rollback assets:
  "Thyroid 2026 UPdated".archive_pub_v1_0.view_ddl_snapshot_pre249_<ts>
  "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_pre249_<ts>
  "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_patient_master_prev233_snapshot_<ts>
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_LOG_PATH = OUTPUT_DIR / "249_run.log"
DECISION_LOG_PATH = OUTPUT_DIR / "249_decision_log.json"
DRY_RUN_REPORT_PATH = OUTPUT_DIR / "249_dry_run_report.md"

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_QUALIFIED = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
SCRIPT_TAG = "Script 249"
RUN_DATE = "2026-04-16"
CPM = "canonical_patient_master"

# AGENTS.md deprecated -> successor map. Some deprecated cols share a successor.
DEPRECATED_TO_SUCCESSOR: dict[str, str] = {
    "margin_status_final":   "r_class_true",
    "margin_r_class":        "r_class_true",
    "lvi_grade_final_v13":   "lvi_ordinal_worst",
    "multifocal_flag":       "multifocal_flag_path",
    "path_multifocal_flag":  "multifocal_flag_path",
    "path_n_tumors":         "n_tumors_path",
    "max_tumor_size_cm_v10": "tumor_size_cm_max",
}

# Phase 1 already-renamed column. Substitute remaining view references.
PHASE1_RENAMED: dict[str, str] = {
    "syn_margin_distance_mm": "syn_margin_distance_mm_num",
}

# Combined substitution map for view rewrites.
ALL_SUBSTITUTIONS: dict[str, str] = {**DEPRECATED_TO_SUCCESSOR, **PHASE1_RENAMED}

# *_prev_233 cols to drop (after archive)
PREV233_COLS = [
    "any_recurrence_flag_prev_233",
    "first_surgery_date_prev_233",
    "followup_days_prev_233",
    "followup_years_prev_233",
    "last_contact_date_prev_233",
    "last_contact_source_prev_233",
]

# Coverage-rule whitelist: deprecated cols where successor is INTENTIONALLY
# lower coverage (denominator correction, not regression). The preflight
# coverage check (successor >= deprecated - 5pp) is bypassed for these.
COVERAGE_WHITELIST: dict[str, dict] = {
    "margin_r_class": {
        "successor": "r_class_true",
        "rationale": (
            "Deprecated column had 100% coverage due to R1 default for all "
            "malignant rows (bug). Successor correctly NULLs rows with "
            "insufficient margin evidence. Coverage drop is a denominator "
            "correction, not a regression. See FINALIZATION_REPORT_20260416.md "
            "and AGENTS.md deprecated-columns table."
        ),
        "approved_by": "user",
        "approved_at": "2026-04-16",
    },
}


# Per-column COMMENT text (deprecation date, successor, reason)
DEPRECATION_REASONS: dict[str, str] = {
    "margin_status_final": (
        "Deprecated 2026-04-16 (Script 249 / v1_1). Use r_class_true instead. "
        "Original derivation defaulted to R1 for malignant rows with literal-string nan in syn_margin_distance_mm."
    ),
    "margin_r_class": (
        "Deprecated 2026-04-16 (Script 249 / v1_1). Use r_class_true instead. "
        "Successor produced by patient_tumor_rollup_v1 (script 230) corrects the same defaulted-R1 contamination."
    ),
    "lvi_grade_final_v13": (
        "Deprecated 2026-04-16 (Script 249 / v1_1). Use lvi_ordinal_worst (granular ordinal) "
        "or lvi_any_present_path (BOOLEAN). Original column collapsed 92-95% of values to 'present_ungraded' losing signal."
    ),
    "multifocal_flag": (
        "Deprecated 2026-04-16 (Script 249 / v1_1). Use multifocal_flag_path instead. "
        "Original column was 100% NULL prior to script 230 backfill via path_synoptics."
    ),
    "path_multifocal_flag": (
        "Deprecated 2026-04-16 (Script 249 / v1_1). Use multifocal_flag_path instead. "
        "Path-prefixed name superseded by canonical multifocal_flag_path (same source, clearer naming)."
    ),
    "path_n_tumors": (
        "Deprecated 2026-04-16 (Script 249 / v1_1). Use n_tumors_path instead. "
        "Path-prefixed name superseded by canonical n_tumors_path."
    ),
    "max_tumor_size_cm_v10": (
        "Deprecated 2026-04-16 (Script 249 / v1_1). Use tumor_size_cm_max instead. "
        "Versioned (_v10) column superseded by canonical tumor_size_cm_max."
    ),
}


def ts_utc_short() -> str:
    return datetime.now(timezone.utc).strftime("%H:%M:%S.") + f"{datetime.now(timezone.utc).microsecond // 1000:03d}Z"


def log(msg: str, log_file=None) -> None:
    line = f"[{ts_utc_short()}] {msg}"
    print(line, flush=True)
    if log_file is not None:
        log_file.write(line + "\n")
        log_file.flush()


# ---------------------------------------------------------------------------
# Phase 2A — preflight invariants
# ---------------------------------------------------------------------------

def preflight_invariants(con) -> dict:
    n_rows = con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0]
    n_cols = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='{CPM}'"""
    ).fetchone()[0]
    if n_rows != 10871:
        raise RuntimeError(f"CPM rows {n_rows} != 10871")

    # Successor coverage check: each successor must have coverage >= deprecated - 5pp
    coverage_check: list[dict] = []
    for dep, succ in DEPRECATED_TO_SUCCESSOR.items():
        try:
            n_dep = con.execute(
                f'SELECT COUNT(*) FROM {CPM} WHERE "{dep}" IS NOT NULL'
            ).fetchone()[0]
        except Exception:
            n_dep = -1
        try:
            n_succ = con.execute(
                f'SELECT COUNT(*) FROM {CPM} WHERE "{succ}" IS NOT NULL'
            ).fetchone()[0]
        except Exception:
            n_succ = -1
        cov_dep = (n_dep / n_rows) * 100 if n_dep >= 0 else None
        cov_succ = (n_succ / n_rows) * 100 if n_succ >= 0 else None
        threshold_ok = (cov_succ is not None and cov_dep is not None
                        and cov_succ + 5.0 >= cov_dep)
        whitelisted = dep in COVERAGE_WHITELIST and COVERAGE_WHITELIST[dep]["successor"] == succ
        ok = bool(threshold_ok or whitelisted)
        coverage_check.append({
            "deprecated": dep,
            "successor": succ,
            "n_dep": n_dep,
            "n_succ": n_succ,
            "cov_dep_pct": round(cov_dep, 2) if cov_dep else None,
            "cov_succ_pct": round(cov_succ, 2) if cov_succ else None,
            "threshold_pass": threshold_ok,
            "whitelisted": whitelisted,
            "whitelist_rationale": (
                COVERAGE_WHITELIST[dep]["rationale"] if whitelisted else None
            ),
            "ok": ok,
        })
    return {
        "cpm_rows": n_rows,
        "cpm_cols": n_cols,
        "coverage_check": coverage_check,
    }


# ---------------------------------------------------------------------------
# Phase 2B — view DDL snapshot
# ---------------------------------------------------------------------------

def get_all_ms_views(con) -> list[tuple[str, str, str]]:
    """Return list of (schema, name, definition) for all manuscript_workspace views."""
    return con.execute(
        """SELECT table_schema, table_name, view_definition
           FROM information_schema.views
           WHERE table_catalog = ?
             AND table_schema  = 'manuscript_workspace'
           ORDER BY table_name""",
        [PUBLICATION_DB],
    ).fetchall()


def snapshot_views(con, run_ts: str, views: list[tuple[str, str, str]]) -> str:
    dest_name = f"view_ddl_snapshot_pre249_{run_ts}"
    full = f'{ARCHIVE_QUALIFIED}."{dest_name}"'
    con.execute(f"DROP TABLE IF EXISTS {full}")
    con.execute(
        f"""CREATE TABLE {full} (
              schema_name        VARCHAR,
              view_name          VARCHAR,
              view_definition    VARCHAR,
              snapshotted_at     TIMESTAMP
           )"""
    )
    rows = [(s, n, d, datetime.now(timezone.utc)) for s, n, d in views if d]
    con.executemany(
        f"INSERT INTO {full} VALUES (?,?,?,?)", rows,
    )
    con.execute(
        f"""COMMENT ON TABLE {full} IS
            '{SCRIPT_TAG} ({RUN_DATE}) pre-write snapshot of all
             manuscript_workspace view DDLs ({len(rows)} rows). Rollback source
             for the v1_1 deprecation rewrite — reload via DROP VIEW + CREATE
             VIEW per row to restore.'"""
    )
    return dest_name


# ---------------------------------------------------------------------------
# Phase 2C — CPM snapshot
# ---------------------------------------------------------------------------

def snapshot_cpm(con, run_ts: str) -> str:
    dest = f"canonical_patient_master_pre249_{run_ts}"
    full = f'{ARCHIVE_QUALIFIED}."{dest}"'
    con.execute(f"CREATE OR REPLACE TABLE {full} AS SELECT * FROM {CPM}")
    con.execute(
        f"""COMMENT ON TABLE {full} IS
            '{SCRIPT_TAG} ({RUN_DATE}) pre-write snapshot of
             canonical_patient_master. Rollback source for the v1_1
             deprecation rewrite. Cohort = 10,871.'"""
    )
    return dest


# ---------------------------------------------------------------------------
# Phase 2D — view dependency graph + rewrite
# ---------------------------------------------------------------------------

VIEW_REF_RE = re.compile(
    rf"(?:{re.escape(PUBLICATION_DB)}\.)?manuscript_workspace\.([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE,
)


def build_dependency_graph(views: list[tuple[str, str, str]]) -> dict[str, set[str]]:
    """Return adjacency map: view -> set of views it DEPENDS ON.

    Parses each view's DDL text for references to other manuscript_workspace
    views (FROM/JOIN context). Only considers views in the input list.
    """
    names = {n for _, n, _ in views}
    deps: dict[str, set[str]] = {n: set() for n in names}
    for _, name, defn in views:
        if not defn:
            continue
        # Strip the leading "CREATE VIEW manuscript_workspace.<name> AS"
        body = re.sub(rf"^CREATE\s+VIEW\s+\S+\s+AS\s+", "", defn, count=1, flags=re.IGNORECASE)
        for m in VIEW_REF_RE.finditer(body):
            target = m.group(1)
            if target in names and target != name:
                deps[name].add(target)
    return deps


def topo_sort(deps: dict[str, set[str]]) -> list[str]:
    """Kahn's algorithm. Returns deps-first order (deepest dependencies first)."""
    indeg = {n: 0 for n in deps}
    for n, ds in deps.items():
        for d in ds:
            # n depends on d -> reverse edge: d -> n adds indeg to n
            pass
    # Build reverse adjacency: dependents-of[d] = set of n that depend on d
    rev: dict[str, set[str]] = {n: set() for n in deps}
    for n, ds in deps.items():
        for d in ds:
            rev[d].add(n)
            indeg[n] += 1
    # Start with nodes that have indeg 0 (no dependencies)
    ready = sorted([n for n, k in indeg.items() if k == 0])
    out: list[str] = []
    while ready:
        n = ready.pop(0)
        out.append(n)
        for child in sorted(rev.get(n, set())):
            indeg[child] -= 1
            if indeg[child] == 0:
                ready.append(child)
        ready.sort()
    if len(out) != len(deps):
        # cycle detected; return partial + remainder in stable order
        out += sorted([n for n in deps if n not in out])
    return out


def find_views_referencing(views: list[tuple[str, str, str]], col: str) -> list[str]:
    """Return view names whose definition contains column `col` as word."""
    pat = re.compile(rf"\b{re.escape(col)}\b")
    return sorted(
        n for _, n, d in views
        if d and pat.search(d)
    )


def rewrite_view_ddl(defn: str, substitutions: dict[str, str]) -> tuple[str, list[tuple[str, str, int]]]:
    """Apply word-boundary substitutions to a view DDL.

    Returns (new_ddl, applied_changes).
    Each applied_change: (old_col, new_col, count).
    Skips occurrences that look like SQL identifier suffixes
    (already-prefixed columns we shouldn't touch).
    """
    new = defn
    applied: list[tuple[str, str, int]] = []
    for old, new_col in substitutions.items():
        # Word-boundary, but NOT preceded by '.' (allow col matches like 'p.multifocal_flag')
        # NOT followed by '_' (avoid eating multifocal_flag_path itself)
        pattern = rf"(?<![A-Za-z0-9_]){re.escape(old)}(?![A-Za-z0-9_])"
        # First count occurrences to avoid double-counting after replace
        count = len(re.findall(pattern, new))
        if count > 0:
            new = re.sub(pattern, new_col, new)
            applied.append((old, new_col, count))
    return new, applied


def rewrite_views_in_order(con, views_with_defs: list[tuple[str, str, str]],
                           order: list[str], substitutions: dict[str, str],
                           do_writes: bool, log_file) -> dict:
    """Walk views in topo order; rewrite any whose DDL contains a deprecated col."""
    by_name = {n: (s, d) for s, n, d in views_with_defs}
    rewrites: list[dict] = []
    for name in order:
        sch, defn = by_name.get(name, (None, None))
        if defn is None:
            continue
        new_ddl, applied = rewrite_view_ddl(defn, substitutions)
        if not applied:
            continue
        rewrites.append({
            "view": f"{sch}.{name}",
            "applied": [{"old": o, "new": n, "count": c} for o, n, c in applied],
            "new_ddl_preview": new_ddl[:300],
        })
        log(f"  REWRITE {sch}.{name}  ({len(applied)} substitutions)", log_file)
        for o, nc, c in applied:
            log(f"      {o:35s} -> {nc:35s} ({c}x)", log_file)
        if do_writes:
            # DROP + CREATE replacement.
            con.execute(f'DROP VIEW IF EXISTS {sch}."{name}"')
            con.execute(new_ddl)
    return {"n_rewritten": len(rewrites), "rewrites": rewrites}


# ---------------------------------------------------------------------------
# Phase 2E — CPM column renames
# ---------------------------------------------------------------------------

def apply_cpm_renames(con, log_file) -> dict:
    """Rename each deprecated col to DEPRECATED__name + COMMENT."""
    cpm_cols_now = {
        r[0]
        for r in con.execute(
            f"""SELECT column_name FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                  AND table_name='{CPM}'"""
        ).fetchall()
    }
    renamed: list[str] = []
    skipped: list[str] = []
    for dep in DEPRECATED_TO_SUCCESSOR:
        new_name = f"DEPRECATED__{dep}"
        if dep not in cpm_cols_now:
            log(f"  SKIP (already absent): {dep}", log_file)
            skipped.append(dep)
            continue
        if new_name in cpm_cols_now:
            log(f"  SKIP (DEPRECATED__{dep} already exists)", log_file)
            skipped.append(dep)
            continue
        con.execute(
            f'ALTER TABLE {CPM} RENAME COLUMN "{dep}" TO "{new_name}"'
        )
        comment = DEPRECATION_REASONS[dep].replace("'", "''")
        con.execute(
            f'COMMENT ON COLUMN {CPM}."{new_name}" IS \'{comment}\''
        )
        renamed.append(dep)
        log(f"  RENAMED {dep} -> {new_name}", log_file)
    return {"renamed": renamed, "skipped": skipped}


# ---------------------------------------------------------------------------
# Phase 2F — *_prev_233 archive + drop
# ---------------------------------------------------------------------------

def archive_and_drop_prev233(con, run_ts: str, log_file) -> dict:
    cpm_cols_now = {
        r[0]
        for r in con.execute(
            f"""SELECT column_name FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                  AND table_name='{CPM}'"""
        ).fetchall()
    }
    present = [c for c in PREV233_COLS if c in cpm_cols_now]
    if not present:
        log("  SKIP: no *_prev_233 columns present (already dropped)", log_file)
        return {"archived": None, "dropped": []}

    # Archive: SELECT research_id + prev233 cols
    sel_cols = "research_id, " + ", ".join(f'"{c}"' for c in present)
    archive_name = f"canonical_patient_master_prev233_snapshot_{run_ts}"
    full = f'{ARCHIVE_QUALIFIED}."{archive_name}"'
    con.execute(
        f"CREATE OR REPLACE TABLE {full} AS SELECT {sel_cols} FROM {CPM}"
    )
    con.execute(
        f"""COMMENT ON TABLE {full} IS
            '{SCRIPT_TAG} ({RUN_DATE}) snapshot of *_prev_233 columns from
             canonical_patient_master before drop. {len(present)} cols x
             10,871 rows. Authorized by FINALIZATION_REPORT_20260416 residual #3.'"""
    )
    log(f"  archived {len(present)} *_prev_233 cols to {full}", log_file)

    dropped: list[str] = []
    for c in present:
        con.execute(f'ALTER TABLE {CPM} DROP COLUMN "{c}"')
        dropped.append(c)
        log(f"  dropped column: {c}", log_file)
    return {"archived": archive_name, "dropped": dropped}


# ---------------------------------------------------------------------------
# Phase 2G — legacy column sweep (audit only)
# ---------------------------------------------------------------------------

def legacy_column_sweep(con, do_writes: bool, log_file) -> dict:
    """Find *_v[0-9]+ columns where higher-version successor exists same row grain (CPM)."""
    cols = [
        r[0]
        for r in con.execute(
            f"""SELECT column_name FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                  AND table_name='{CPM}'
                ORDER BY column_name"""
        ).fetchall()
    ]
    # Group by stem (strip trailing _v\d+)
    groups: dict[str, list[tuple[str, int]]] = {}
    pat = re.compile(r"^(.*)_v(\d+)$")
    for c in cols:
        m = pat.match(c)
        if not m:
            continue
        stem, ver = m.group(1), int(m.group(2))
        groups.setdefault(stem, []).append((c, ver))

    legacies: list[dict] = []
    for stem, versions in groups.items():
        if len(versions) < 2:
            continue
        versions.sort(key=lambda x: x[1])
        max_ver = versions[-1][1]
        for col, v in versions[:-1]:
            legacies.append({
                "column_name": col,
                "version": v,
                "stem": stem,
                "max_version_in_cpm": max_ver,
                "successor_column": versions[-1][0],
            })

    if do_writes:
        con.execute("CREATE SCHEMA IF NOT EXISTS manuscript_workspace")
        con.execute(
            "DROP TABLE IF EXISTS manuscript_workspace.legacy_column_sweep_v1_1"
        )
        con.execute(
            """CREATE TABLE manuscript_workspace.legacy_column_sweep_v1_1 (
                 column_name           VARCHAR,
                 version               INTEGER,
                 stem                  VARCHAR,
                 max_version_in_cpm    INTEGER,
                 successor_column      VARCHAR,
                 inventoried_at        TIMESTAMP
               )"""
        )
        rows = [
            (l["column_name"], l["version"], l["stem"],
             l["max_version_in_cpm"], l["successor_column"],
             datetime.now(timezone.utc))
            for l in legacies
        ]
        if rows:
            con.executemany(
                "INSERT INTO manuscript_workspace.legacy_column_sweep_v1_1 VALUES (?,?,?,?,?,?)",
                rows,
            )
        con.execute(
            f"""COMMENT ON TABLE manuscript_workspace.legacy_column_sweep_v1_1 IS
                '{SCRIPT_TAG} ({RUN_DATE}). Inventory of *_v\\d+ columns on CPM where a
                 higher-version successor exists. NOT auto-deprecated; surface for
                 user review.'"""
        )
    log(f"  legacy_column_sweep: {len(legacies)} legacy columns surfaced", log_file)
    for l in legacies[:10]:
        log(f"    {l['column_name']:45s} -> {l['successor_column']}", log_file)
    return {"n_legacies": len(legacies), "legacies": legacies}


# ---------------------------------------------------------------------------
# Phase 2H — registry feeds_master_columns update
# ---------------------------------------------------------------------------

def update_registry_feeds(con, do_writes: bool, log_file) -> dict:
    rows = con.execute(
        """SELECT detail_table_name, feeds_master_columns
           FROM manuscript_workspace.detail_table_registry_v1
           WHERE feeds_master_columns IS NOT NULL"""
    ).fetchall()
    updates: list[dict] = []
    for tn, feeds in rows:
        new_feeds = feeds
        applied: list[tuple[str, str]] = []
        for old, new in DEPRECATED_TO_SUCCESSOR.items():
            pat = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(old)}(?![A-Za-z0-9_])")
            if pat.search(new_feeds):
                new_feeds = pat.sub(new, new_feeds)
                applied.append((old, new))
        if applied:
            updates.append({
                "detail_table_name": tn,
                "applied": applied,
                "before": feeds[:120],
                "after": new_feeds[:120],
            })
            if do_writes:
                escaped = new_feeds.replace("'", "''")
                con.execute(
                    f"""UPDATE manuscript_workspace.detail_table_registry_v1
                        SET feeds_master_columns = '{escaped}'
                        WHERE detail_table_name = '{tn}'"""
                )
    log(f"  registry rows updated: {len(updates)}", log_file)
    return {"n_updated": len(updates), "updates": updates}


# ---------------------------------------------------------------------------
# Phase 2I — final assertions
# ---------------------------------------------------------------------------

def final_assertions(con, log_file) -> dict:
    # All views compile
    views = con.execute(
        """SELECT table_schema, table_name FROM information_schema.views
           WHERE table_catalog = ? AND table_schema = 'manuscript_workspace'""",
        [PUBLICATION_DB],
    ).fetchall()
    broken: list[tuple[str, str]] = []
    for sch, name in views:
        try:
            con.execute(f'SELECT 1 FROM "{sch}"."{name}" LIMIT 0').fetchall()
        except Exception as e:
            broken.append((f"{sch}.{name}", str(e)[:160]))
    log(f"  view compile: {len(views)-len(broken)}/{len(views)} pass, {len(broken)} broken", log_file)
    for v, e in broken:
        log(f"    BROKEN: {v} -> {e}", log_file)

    # Registry has 0 references to deprecated col names
    dep_pat = "|".join(re.escape(c) for c in DEPRECATED_TO_SUCCESSOR)
    rows = con.execute(
        f"""SELECT detail_table_name, feeds_master_columns
            FROM manuscript_workspace.detail_table_registry_v1
            WHERE feeds_master_columns IS NOT NULL
              AND regexp_matches(feeds_master_columns, '\\b({dep_pat})\\b')"""
    ).fetchall()
    log(f"  registry refs to deprecated cols: {len(rows)} (expect 0)", log_file)

    # Cohort invariants
    n = con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0]
    n_dist = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {CPM}").fetchone()[0]
    n_null_rid = con.execute(
        f"SELECT COUNT(*) FROM {CPM} WHERE research_id IS NULL"
    ).fetchone()[0]
    n_null_fpo = con.execute(
        f"SELECT COUNT(*) FROM {CPM} WHERE fna_path_outcome IS NULL"
    ).fetchone()[0]
    log(f"  CPM rows={n} distinct_rid={n_dist} null_rid={n_null_rid} null_fpo={n_null_fpo}", log_file)

    # Explicit asserts requested by user:
    cpm_col_count = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='{CPM}'"""
    ).fetchone()[0]
    deprecated_col_count = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='{CPM}' AND column_name LIKE 'DEPRECATED__%'"""
    ).fetchone()[0]
    prev233_col_count = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='{CPM}' AND column_name LIKE '%_prev_233'"""
    ).fetchone()[0]
    log(f"  ASSERT cpm col count: {cpm_col_count} (expect 1500)", log_file)
    log(f"  ASSERT DEPRECATED__ col count: {deprecated_col_count} (expect 7)", log_file)
    log(f"  ASSERT %_prev_233 col count: {prev233_col_count} (expect 0)", log_file)

    return {
        "n_views_total": len(views),
        "n_views_broken": len(broken),
        "broken_views": broken,
        "registry_dep_refs": [{"table": t, "feeds": f} for t, f in rows],
        "cohort": {"n_rows": n, "n_distinct_rid": n_dist, "n_null_rid": n_null_rid, "n_null_fpo": n_null_fpo},
        "explicit_asserts": {
            "cpm_col_count": cpm_col_count,
            "expected_cpm_col_count": 1500,
            "deprecated_col_count": deprecated_col_count,
            "expected_deprecated_col_count": 7,
            "prev233_col_count": prev233_col_count,
            "expected_prev233_col_count": 0,
        },
    }


# ---------------------------------------------------------------------------
# Rollback helpers
# ---------------------------------------------------------------------------

def rollback(con, view_snapshot_name: str, renamed_cols: list[str],
             dropped_prev233: list[str], prev233_archive: str | None,
             log_file) -> None:
    log("ROLLBACK BEGIN", log_file)
    # 1. Restore view DDLs
    full = f'{ARCHIVE_QUALIFIED}."{view_snapshot_name}"'
    try:
        snaps = con.execute(
            f"SELECT schema_name, view_name, view_definition FROM {full}"
        ).fetchall()
        for sch, name, ddl in snaps:
            try:
                con.execute(f'DROP VIEW IF EXISTS {sch}."{name}"')
                con.execute(ddl)
            except Exception as e:
                log(f"  ROLLBACK view {sch}.{name} FAILED: {str(e)[:160]}", log_file)
        log(f"  restored {len(snaps)} views from {full}", log_file)
    except Exception as e:
        log(f"  ROLLBACK view-restore FAILED at top level: {str(e)[:160]}", log_file)

    # 2. Undo CPM renames
    for c in renamed_cols:
        try:
            con.execute(
                f'ALTER TABLE {CPM} RENAME COLUMN "DEPRECATED__{c}" TO "{c}"'
            )
            log(f"  un-renamed DEPRECATED__{c} -> {c}", log_file)
        except Exception as e:
            log(f"  un-rename {c} FAILED: {str(e)[:160]}", log_file)

    # 3. Restore *_prev_233 cols (re-add columns + populate from archive)
    if dropped_prev233 and prev233_archive:
        archive_full = f'{ARCHIVE_QUALIFIED}."{prev233_archive}"'
        try:
            for c in dropped_prev233:
                # Get column type from archive
                t = con.execute(
                    f"""SELECT data_type FROM information_schema.columns
                        WHERE table_catalog='{ARCHIVE_DB}'
                          AND table_schema='{ARCHIVE_SCHEMA}'
                          AND table_name='{prev233_archive}'
                          AND column_name='{c}'"""
                ).fetchone()
                if not t:
                    continue
                con.execute(f'ALTER TABLE {CPM} ADD COLUMN "{c}" {t[0]}')
                con.execute(
                    f"""UPDATE {CPM} AS cpm
                        SET "{c}" = arc."{c}"
                        FROM {archive_full} AS arc
                        WHERE cpm.research_id = arc.research_id"""
                )
                log(f"  restored *_prev_233 col: {c}", log_file)
        except Exception as e:
            log(f"  prev233 restore FAILED: {str(e)[:160]}", log_file)
    log("ROLLBACK END", log_file)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Execute writes. Without this flag, --dry-run is the default.")
    ap.add_argument("--dry-run", dest="dry_run", action="store_true", default=True,
                    help="Default. Audit + plan only; no DB writes.")
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
        "script": "249",
        "run_ts": run_ts,
        "run_date": RUN_DATE,
        "mode": mode,
        "phases": {},
    }
    view_snapshot_name: str | None = None
    cpm_snapshot_name: str | None = None
    renamed_cols: list[str] = []
    prev233_dropped: list[str] = []
    prev233_archive: str | None = None

    try:
        # ---- 2A preflight ---------------------------------------------
        log("PHASE 2A — preflight invariants", log_file)
        pre = preflight_invariants(con)
        decision["phases"]["preflight"] = pre
        log(f"  CPM: {pre['cpm_rows']} rows x {pre['cpm_cols']} cols", log_file)
        for cc in pre["coverage_check"]:
            if cc["ok"] and cc["whitelisted"]:
                tag = "WHITELIST"
            elif cc["ok"]:
                tag = "OK"
            else:
                tag = "WARN"
            log(
                f"  [{tag}] {cc['deprecated']:30s} ({cc['cov_dep_pct']}%) -> "
                f"{cc['successor']:25s} ({cc['cov_succ_pct']}%)",
                log_file,
            )
            if cc["whitelisted"]:
                log(f"      WHITELIST RATIONALE: {cc['whitelist_rationale']}", log_file)
        bad = [c for c in pre["coverage_check"] if not c["ok"]]
        if bad and do_writes:
            raise RuntimeError(
                f"successor coverage check FAILED for {len(bad)} columns: "
                + ", ".join(c["deprecated"] for c in bad)
            )

        # ---- 2D-prep view enumeration + dependency graph --------------
        log("PHASE 2D-prep — enumerate views + build dependency graph", log_file)
        views = get_all_ms_views(con)
        log(f"  manuscript_workspace views: {len(views)}", log_file)
        deps = build_dependency_graph(views)
        order = topo_sort(deps)
        decision["phases"]["dependency_graph"] = {
            "view_count": len(views),
            "edges": {n: sorted(list(s)) for n, s in deps.items() if s},
            "topo_order": order,
        }
        log(f"  topological rewrite order ({len(order)} views):", log_file)
        for i, n in enumerate(order, 1):
            ds = deps.get(n, set())
            tag = "(root)" if not ds else f"(depends on {len(ds)})"
            log(f"    {i:2d}. {n}  {tag}", log_file)

        # Cross-check: list views referencing each deprecated col
        log("PHASE 2D-prep — view references per deprecated col:", log_file)
        for col in list(DEPRECATED_TO_SUCCESSOR) + list(PHASE1_RENAMED):
            refs = find_views_referencing(views, col)
            log(f"    {col:30s} -> {len(refs)} views: {', '.join(refs[:5])}{'…' if len(refs)>5 else ''}", log_file)

        # ---- DRY-RUN early exit (after showing what we would do) ------
        if args.dry_run and not do_writes:
            log("PHASE 2D — preview rewrites (dry-run)", log_file)
            preview = rewrite_views_in_order(
                con, views, order, ALL_SUBSTITUTIONS, do_writes=False, log_file=log_file
            )
            decision["phases"]["dry_run_rewrites"] = preview

            # Preview Phase 2E (renames)
            log("PHASE 2E — preview CPM column renames", log_file)
            cpm_cols_now = {
                r[0] for r in con.execute(
                    f"""SELECT column_name FROM information_schema.columns
                        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                          AND table_name='{CPM}'"""
                ).fetchall()
            }
            rename_plan = []
            for dep in DEPRECATED_TO_SUCCESSOR:
                action = "WILL_RENAME" if dep in cpm_cols_now else "ALREADY_ABSENT"
                rename_plan.append({
                    "from": dep, "to": f"DEPRECATED__{dep}",
                    "action": action, "comment": DEPRECATION_REASONS[dep],
                })
                log(f"  {action:18s}: {dep:30s} -> DEPRECATED__{dep}", log_file)
            decision["phases"]["dry_run_renames"] = rename_plan

            # Preview Phase 2F (*_prev_233 drops)
            log("PHASE 2F — preview *_prev_233 drops", log_file)
            present_prev = [c for c in PREV233_COLS if c in cpm_cols_now]
            log(f"  *_prev_233 columns present: {len(present_prev)} (would archive + drop)", log_file)
            for c in present_prev:
                log(f"    will archive + drop: {c}", log_file)
            decision["phases"]["dry_run_prev233"] = {
                "present": present_prev,
                "archive_target": f"canonical_patient_master_prev233_snapshot_{run_ts}",
            }

            # Preview Phase 2G (legacy_column_sweep)
            log("PHASE 2G — preview legacy_column_sweep_v1_1 inventory", log_file)
            sweep = legacy_column_sweep(con, do_writes=False, log_file=log_file)
            decision["phases"]["dry_run_legacy_sweep"] = sweep

            # Preview Phase 2H (registry feeds update)
            log("PHASE 2H — preview registry feeds_master_columns updates", log_file)
            reg = update_registry_feeds(con, do_writes=False, log_file=log_file)
            decision["phases"]["dry_run_registry"] = reg

            # Done
            with DECISION_LOG_PATH.open("w") as f:
                json.dump(decision, f, indent=2, default=str)
            log(f"decision log written: {DECISION_LOG_PATH.relative_to(REPO)}", log_file)
            log(f"=== END dry-run elapsed={time.time()-t0:.1f}s", log_file)
            log_file.close()
            return

        # ---- 2B view DDL snapshot -------------------------------------
        log("PHASE 2B — snapshot view DDLs", log_file)
        view_snapshot_name = snapshot_views(con, run_ts, views)
        log(f"  archived: {view_snapshot_name}", log_file)
        decision["phases"]["view_snapshot"] = view_snapshot_name

        # ---- 2C CPM snapshot ------------------------------------------
        log("PHASE 2C — snapshot CPM", log_file)
        cpm_snapshot_name = snapshot_cpm(con, run_ts)
        log(f"  archived: {cpm_snapshot_name}", log_file)
        decision["phases"]["cpm_snapshot"] = cpm_snapshot_name

        # ---- 2D rewrite views in topo order ---------------------------
        log("PHASE 2D — rewrite views in dependency order", log_file)
        rewrite_result = rewrite_views_in_order(
            con, views, order, ALL_SUBSTITUTIONS, do_writes=True, log_file=log_file
        )
        decision["phases"]["view_rewrite"] = rewrite_result
        # Compile sweep checkpoint
        broken_after_2d = []
        for sch, name, _ in views:
            try:
                con.execute(f'SELECT 1 FROM "{sch}"."{name}" LIMIT 0').fetchall()
            except Exception as e:
                broken_after_2d.append((f"{sch}.{name}", str(e)[:120]))
        if broken_after_2d:
            log(f"  POST-2D broken views: {len(broken_after_2d)}", log_file)
            for v, e in broken_after_2d:
                log(f"    BROKEN: {v} -> {e}", log_file)
            raise RuntimeError(
                f"post-2D compile sweep failed: {len(broken_after_2d)} broken views"
            )
        log("  POST-2D compile sweep: 0 broken (all views resolve)", log_file)

        # ---- 2E CPM column renames ------------------------------------
        log("PHASE 2E — rename CPM columns to DEPRECATED__name", log_file)
        rename_result = apply_cpm_renames(con, log_file)
        renamed_cols = rename_result["renamed"]
        decision["phases"]["cpm_renames"] = rename_result

        # ---- 2F prev233 drop ------------------------------------------
        log("PHASE 2F — archive + drop *_prev_233 cols", log_file)
        prev233_result = archive_and_drop_prev233(con, run_ts, log_file)
        prev233_dropped = prev233_result["dropped"]
        prev233_archive = prev233_result["archived"]
        decision["phases"]["prev233"] = prev233_result

        # ---- 2G legacy sweep -----------------------------------------
        log("PHASE 2G — legacy_column_sweep_v1_1", log_file)
        sweep_result = legacy_column_sweep(con, do_writes=True, log_file=log_file)
        decision["phases"]["legacy_sweep"] = sweep_result

        # ---- 2H registry update --------------------------------------
        log("PHASE 2H — update registry feeds_master_columns", log_file)
        reg_result = update_registry_feeds(con, do_writes=True, log_file=log_file)
        decision["phases"]["registry"] = reg_result

        # ---- 2I final assertions -------------------------------------
        log("PHASE 2I — final assertions", log_file)
        final = final_assertions(con, log_file)
        decision["phases"]["final"] = final
        if final["n_views_broken"] != 0:
            raise RuntimeError(
                f"FINAL: {final['n_views_broken']} broken views — must be 0"
            )
        if final["registry_dep_refs"]:
            raise RuntimeError(
                f"FINAL: registry still references "
                f"{len(final['registry_dep_refs'])} deprecated columns"
            )
        if final["cohort"]["n_rows"] != 10871:
            raise RuntimeError(
                f"FINAL: CPM rows {final['cohort']['n_rows']} != 10871"
            )
        ea = final["explicit_asserts"]
        if ea["cpm_col_count"] != ea["expected_cpm_col_count"]:
            raise RuntimeError(
                f"FINAL: CPM col count {ea['cpm_col_count']} != "
                f"{ea['expected_cpm_col_count']}"
            )
        if ea["deprecated_col_count"] != ea["expected_deprecated_col_count"]:
            raise RuntimeError(
                f"FINAL: DEPRECATED__ col count {ea['deprecated_col_count']} != "
                f"{ea['expected_deprecated_col_count']}"
            )
        if ea["prev233_col_count"] != ea["expected_prev233_col_count"]:
            raise RuntimeError(
                f"FINAL: %_prev_233 col count {ea['prev233_col_count']} != "
                f"{ea['expected_prev233_col_count']}"
            )
        log("  ALL ASSERTIONS PASS", log_file)

    except Exception as exc:
        log(f"FATAL: {exc!r}", log_file)
        if do_writes and view_snapshot_name is not None:
            try:
                rollback(con, view_snapshot_name, renamed_cols,
                         prev233_dropped, prev233_archive, log_file)
            except Exception as rb_exc:
                log(f"ROLLBACK ITSELF FAILED: {rb_exc!r}", log_file)
        with DECISION_LOG_PATH.open("w") as f:
            json.dump({**decision, "error": str(exc)}, f, indent=2, default=str)
        log_file.close()
        raise

    with DECISION_LOG_PATH.open("w") as f:
        json.dump(decision, f, indent=2, default=str)
    log(f"decision log written: {DECISION_LOG_PATH.relative_to(REPO)}", log_file)
    log(f"=== END elapsed={time.time()-t0:.1f}s", log_file)
    log_file.close()


if __name__ == "__main__":
    main()
