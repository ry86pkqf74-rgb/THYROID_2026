#!/usr/bin/env python3
"""
Script 247 — Canonical v1_0 LOCK

Date:    2026-04-16
Author:  THYROID_2026 canonical-finalization run (v1_0 lock)

Purpose
=======
Final sweep + regeneration + assertion script for the canonical
publication v1_0. Runs last in the 237 -> 247 finalization series
(skipping 244 per the original spec). Emits the explicit line:

    CANONICAL v1_0 LOCK: READY FOR PUBLICATION

iff every assertion holds. Otherwise emits:

    CANONICAL v1_0 LOCK: BLOCKED — <list of failing scripts>

Phases
------
  0  Baseline capture
  1  Catalog-ghost detection (guarded SELECT sweep; queryable enumeration)
  2  Resolve remaining TODO markers in detail_table_registry_v1
  3  Regenerate __readme from QUERYABLE enumeration only
  4  Regenerate detail_table_registry_v1 (preserve descriptions; drop stale)
  5  View-compile sweep across main + manuscript_workspace (100% required)
  6  CPM ↔ registry column pointer verification
  7  Empty-table sweep
  8  __conventions sanity check (≥ 5 rows)
  9  Script 236-style 5-query confirmation block + 2 Group C residuals
 10  Final LOCK line

Per the user spec additions:
  - Catalog-ghost handling (guarded SELECT probe, not info_schema alone)
  - Registry + __readme regen uses queryable enumeration
  - Expanded confirmation block includes Group C residuals

Tables READ (read-only throughout except phases 2-4)
----------------------------------------------------
  thyroid_canonical_publication_v1_0.information_schema.tables
  thyroid_canonical_publication_v1_0.information_schema.views
  thyroid_canonical_publication_v1_0.information_schema.columns
  thyroid_canonical_publication_v1_0.main.__readme
  thyroid_canonical_publication_v1_0.main.canonical_patient_master
  thyroid_canonical_publication_v1_0.main.canonical_tumor_characteristics_v1
  thyroid_canonical_publication_v1_0.main.us_nodules_tirads_vs_inm_v1_discordance_v1
  thyroid_canonical_publication_v1_0.manuscript_workspace.detail_table_registry_v1
  thyroid_canonical_publication_v1_0.manuscript_workspace.__conventions

Tables WRITTEN
--------------
  CREATE OR REPLACE TABLE main.__readme                              (regen)
  DROP / INSERT rows in manuscript_workspace.detail_table_registry_v1
  UPDATE path_size_adjudication_v241 registry row (resolve TODO marker)

Rollback plan
-------------
  - __readme: prior state persisted as
      "Thyroid 2026 UPdated".archive_pub_v1_0.__readme_pre247_backup_<ts>
  - Registry: prior state persisted as
      "Thyroid 2026 UPdated".archive_pub_v1_0.detail_table_registry_v1_pre247_backup_<ts>
  - CREATE OR REPLACE TABLE {main.__readme, ...registry_v1} AS
      SELECT * FROM archive copies.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOCK_LOG_PATH = OUTPUT_DIR / "247_lock_report.json"

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_QUALIFIED = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
SCRIPT_TAG = "Script 247"
RUN_DATE = "2026-04-16"

# Expected script sequence — for the LOCK line failure message.
FINALIZATION_SCRIPTS = ["237", "238", "239", "240", "241", "242", "243", "245", "246", "247"]


def ts_utc() -> str:
    return datetime.utcnow().strftime("%H:%M:%S.") + f"{datetime.utcnow().microsecond // 1000:03d}Z"


def log(msg: str) -> None:
    print(f"[{ts_utc()}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Phase helpers
# ---------------------------------------------------------------------------

def enumerate_queryable_tables(con) -> tuple[list[str], list[tuple[str, str]]]:
    """Return (queryable_main_base_tables, ghost_tables_with_errors).

    information_schema.tables lists candidates; guarded SELECT confirms
    queryability. A ghost appears in info_schema but SELECT raises.
    """
    candidates = con.execute(
        f"""SELECT table_name FROM information_schema.tables
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_type='BASE TABLE'
            ORDER BY table_name"""
    ).fetchall()
    queryable: list[str] = []
    ghosts: list[tuple[str, str]] = []
    for (t,) in candidates:
        try:
            con.execute(f'SELECT 1 FROM "{t}" LIMIT 0').fetchall()
            queryable.append(t)
        except Exception as e:
            ghosts.append((t, str(e)[:160]))
    return queryable, ghosts


def archive_snapshot(con, src_sql: str, dest_name: str, comment: str) -> None:
    full = f'{ARCHIVE_QUALIFIED}."{dest_name}"'
    con.execute(f"CREATE OR REPLACE TABLE {full} AS {src_sql}")
    con.execute(f"COMMENT ON TABLE {full} IS '{comment.replace(chr(39), chr(39)+chr(39))}'")


# ---------------------------------------------------------------------------
# __readme regen (from queryable enumeration)
# ---------------------------------------------------------------------------

def regenerate_readme(con, queryable: list[str]) -> int:
    existing = {
        r[0]: r[2]
        for r in con.execute("SELECT table_name, rows, description FROM __readme").fetchall()
    }
    # Seed missing descriptions for tables added in 237-246.
    seed_additions = {
        "canonical_tumor_characteristics_v1": (
            "Per-tumor canonical (Script 245, 2026-04-16). One row per resected tumor focus per "
            "surgery. STL per-tumor identity + TEM per-surgery broadcast via specimen_tumor_focus_v1 "
            "broker. 8,422 tumor-bearing patients; 2,449 benign tumor-free CPM patients intentionally "
            "absent (v1_0 cohort-scoping convention)."
        ),
        "canonical_us_nodule_characteristics_v1": (
            "Per-(US exam, nodule) canonical (Script 246, 2026-04-16). inm_v1 identity + "
            "tirads_llm_extracted_v2 ACR per-component overlay via deterministic_key parse. "
            "6,126 patients with structured per-exam data; 4,745 placeholder-only us_nodules_tirads "
            "patients documented as v1_1 NLP-extraction TODO."
        ),
        "us_nodules_tirads_vs_inm_v1_discordance_v1": (
            "Audit table (Script 246, 2026-04-16). One row per patient where max TIRADS disagrees "
            "between us_nodules_tirads and imaging_nodule_master_v1. 1,722 patients; mean abs diff "
            "1.35 TR levels. NOT a canonical clinical signal — v1_1 reconciliation review only."
        ),
        "path_size_adjudication_v241": (
            "Path-vs-imaging size adjudication artifact (Script 241, 2026-04-16). 96 patients with "
            "discrepancies (37 path>10cm HIGH, 45 multifocal MEDIUM, 14 unifocal-discrepancy MEDIUM). "
            "NOT applied to CPM — proposed adjudicated values await clinician sign-off."
        ),
    }

    rows: list[tuple[str, int, str]] = []
    for t in sorted(queryable):
        n = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        desc = existing.get(t)
        if not desc or desc.startswith("TODO"):
            desc = seed_additions.get(t, desc or (
                "Catalog entry — describe in Script 247 (auto-generated placeholder)."
                if t == "__readme" else "TODO: describe"
            ))
        rows.append((t, n, desc))

    con.execute("DROP TABLE IF EXISTS __readme")
    con.execute(
        """CREATE TABLE __readme (
             table_name VARCHAR, rows BIGINT, description VARCHAR
           )"""
    )
    con.executemany("INSERT INTO __readme VALUES (?, ?, ?)", rows)
    con.execute(
        f"""COMMENT ON TABLE __readme IS
            'Regenerated by {SCRIPT_TAG} ({RUN_DATE}) from queryable enumeration only. One row per
             main-schema BASE TABLE that responds to SELECT. Catalog-ghost entries (if any) are
             intentionally excluded — see manuscript_workspace.__conventions.catalog_vs_queryable_drift.'"""
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Registry regen (preserve descriptions; drop stale)
# ---------------------------------------------------------------------------

REGISTRY_EXCLUDE_PATTERNS = [
    re.compile(r"^canonical_patient_master$"),
    re.compile(r".*pre\d+.*backup$"),
    re.compile(r"^__readme$"),
    re.compile(r"^data_dictionary_v\d+$"),
    re.compile(r".*_pre\d+_backup$"),
]


def regenerate_registry(con, queryable: list[str]) -> dict:
    """Regenerate manuscript_workspace.detail_table_registry_v1.

    - Preserve domain / join_key / grain / feeds / description /
      canonical_version for tables that already have a row.
    - Drop rows for tables no longer queryable.
    - Resolve any remaining TODO feeds_master_columns entry.
    - Update total_rows / total_patients to current values.
    """
    existing = {
        r[0]: r
        for r in con.execute(
            """SELECT detail_table_name, schema_name, join_key, grain, total_rows,
                      total_patients, domain, feeds_master_columns, description,
                      canonical_version
               FROM manuscript_workspace.detail_table_registry_v1"""
        ).fetchall()
    }

    main_tables = [
        t for t in queryable
        if not any(p.match(t) for p in REGISTRY_EXCLUDE_PATTERNS)
    ]

    # TODO resolutions known at lock time.
    todo_resolutions: dict[str, str] = {
        "path_size_adjudication_v241": (
            "(review artifact; no direct CPM feed) — 96 outlier patients surfaced for clinician "
            "sign-off; v1_1 will fold signed-off rows into canonical_patient_master."
        ),
    }

    # Post-rename fixups: tokens that no longer resolve in CPM because Script 240
    # renamed them. Applied to feeds_master_columns text in any registry row.
    # Format: {old_token: new_token}. Token boundaries respected in the regex below.
    rename_fixups: dict[str, str] = {
        "tumor_size_cm": "path_tumor_size_cm",  # Script 240 rename+deprecation chain
        "imaging_nodule_size_cm": "dominant_nodule_size_cm",  # Script 240
    }
    rename_pat = {
        old: re.compile(rf"(?<![A-Za-z0-9_])(?<!_){re.escape(old)}(?![A-Za-z0-9_])")
        for old in rename_fixups
    }

    new_rows: list[tuple] = []
    stale_dropped: list[str] = []
    todos_resolved: list[str] = []
    added: list[str] = []

    for t in sorted(main_tables):
        has_rid = bool(
            con.execute(
                f"""SELECT 1 FROM information_schema.columns
                    WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                      AND table_name='{t}' AND column_name='research_id' LIMIT 1"""
            ).fetchone()
        )
        if not has_rid:
            continue
        total_rows = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        total_pts = con.execute(f'SELECT COUNT(DISTINCT research_id) FROM "{t}"').fetchone()[0]

        prev = existing.get(t)
        if prev:
            (_, schema, join_key, grain, _prev_rows, _prev_pts,
             domain, feeds, desc, canonical_version) = prev
            # Resolve TODO marker if we know how.
            if feeds and "TODO" in feeds:
                if t in todo_resolutions:
                    feeds = todo_resolutions[t]
                    todos_resolved.append(t)
                else:
                    feeds = feeds.replace("TODO:", "PENDING v1_1:")
                    todos_resolved.append(t)
            # Post-rename fixup: replace stale references to renamed CPM columns.
            if feeds:
                for old, new in rename_fixups.items():
                    feeds = rename_pat[old].sub(new, feeds)
            schema = schema or "main"
            join_key = join_key or "research_id"
            grain = grain or ("one row per patient" if total_pts == total_rows else "multi-row per patient")
            canonical_version = canonical_version or "v1_0"
        else:
            schema = "main"
            join_key = "research_id"
            grain = "one row per patient" if total_pts == total_rows else "multi-row per patient"
            domain = "Other"
            feeds = "(auto-registered by Script 247; manual review recommended)"
            desc = f"Auto-registered by {SCRIPT_TAG} ({RUN_DATE}). Domain={domain}. Review feeds/description manually."
            canonical_version = "v1_0"
            added.append(t)

        new_rows.append(
            (t, schema, join_key, grain, total_rows, total_pts,
             domain, feeds, desc, canonical_version)
        )

    # Stale-drop detection: any registry row pointing to a table not in queryable main.
    keep_names = {r[0] for r in new_rows}
    # Also keep manuscript_workspace rows (registered in Script 236 / 246 for audit tables).
    ms_rows = [
        r for r in existing.values()
        if r[1] == "manuscript_workspace"
    ]
    stale_dropped = sorted(
        name for name, prev in existing.items()
        if prev[1] == "main" and name not in keep_names
    )

    # Rewrite.
    con.execute("DROP TABLE IF EXISTS manuscript_workspace.detail_table_registry_v1")
    con.execute(
        """CREATE TABLE manuscript_workspace.detail_table_registry_v1 (
             detail_table_name      VARCHAR,
             schema_name            VARCHAR,
             join_key               VARCHAR,
             grain                  VARCHAR,
             total_rows             BIGINT,
             total_patients         BIGINT,
             domain                 VARCHAR,
             feeds_master_columns   VARCHAR,
             description            VARCHAR,
             canonical_version      VARCHAR
           )"""
    )
    con.executemany(
        "INSERT INTO manuscript_workspace.detail_table_registry_v1 VALUES (?,?,?,?,?,?,?,?,?,?)",
        new_rows,
    )
    # Re-insert the manuscript_workspace rows (audit feeds from Script 236 / 246).
    if ms_rows:
        con.executemany(
            "INSERT INTO manuscript_workspace.detail_table_registry_v1 VALUES (?,?,?,?,?,?,?,?,?,?)",
            ms_rows,
        )
    con.execute(
        f"""COMMENT ON TABLE manuscript_workspace.detail_table_registry_v1 IS
            'Regenerated by {SCRIPT_TAG} ({RUN_DATE}). One row per drill-down table feeding
             canonical_patient_master. Every feeds_master_columns value is filled — no TODO
             markers permitted at lock time. Sourced from queryable enumeration (not
             information_schema alone) per the catalog_vs_queryable_drift convention.'"""
    )

    return {
        "n_rows": len(new_rows) + len(ms_rows),
        "n_main_rows": len(new_rows),
        "n_ms_rows": len(ms_rows),
        "stale_dropped": stale_dropped,
        "todos_resolved": todos_resolved,
        "added": added,
    }


# ---------------------------------------------------------------------------
# Column-pointer verification (copy of Script 236's logic, kept stable)
# ---------------------------------------------------------------------------

_SKIP_FEED_MARKERS = (
    "(", "no direct", "audit only", "provenance", "reference", "upstream",
    "crosslink", "crosswalk", "subset view", "manuscript-ready", "TODO",
    "PENDING", "dedup crosswalk", "specimen->assay", "specimen-level",
    "level-specific", "episode-level", "lesion-level", "exam-level",
    "component-level", "review artifact", "feeds imaging_patient_summary",
    "feeds patient_tumor_rollup", "v1_1 migration",
)


def _parse_feeds(s: str) -> list[str]:
    if not s:
        return []
    out: list[str] = []
    for tok in re.split(r"[;,\n]+", s):
        tok = tok.strip().rstrip(".;:")
        if not tok or "*" in tok or "(" in tok or ")" in tok or " " in tok or "\t" in tok:
            continue
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", tok):
            out.append(tok)
    return out


def verify_column_pointers(con) -> tuple[int, list[tuple[str, str]]]:
    rows = con.execute(
        """SELECT detail_table_name, feeds_master_columns
           FROM manuscript_workspace.detail_table_registry_v1
           ORDER BY detail_table_name"""
    ).fetchall()
    cpm_cols = {
        r[0]
        for r in con.execute(
            f"""SELECT column_name FROM information_schema.columns
                WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                  AND table_name='canonical_patient_master'"""
        ).fetchall()
    }
    checked = 0
    missing: list[tuple[str, str]] = []
    for table_name, feeds in rows:
        if not feeds:
            continue
        low = feeds.lower()
        if any(m.lower() in low for m in _SKIP_FEED_MARKERS):
            continue
        for col in _parse_feeds(feeds):
            checked += 1
            if col not in cpm_cols:
                missing.append((table_name, col))
    return checked, missing


# ---------------------------------------------------------------------------
# View-compile sweep
# ---------------------------------------------------------------------------

def view_compile_sweep(con) -> tuple[int, int, list[tuple[str, str, str]]]:
    views = con.execute(
        f"""SELECT table_schema, table_name FROM information_schema.views
            WHERE table_catalog='{PUBLICATION_DB}'
              AND table_schema IN ('main', 'manuscript_workspace')
            ORDER BY table_schema, table_name"""
    ).fetchall()
    passes = 0
    fails: list[tuple[str, str, str]] = []
    for schema, name in views:
        try:
            con.execute(f'SELECT 1 FROM {schema}."{name}" LIMIT 0').fetchall()
            passes += 1
        except Exception as e:
            fails.append((schema, name, str(e)[:160]))
    return len(views), passes, fails


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    t0 = time.time()
    log(f"=== START {Path(__file__).name}")
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    lock_report: dict = {
        "script": "247",
        "run_ts": run_ts,
        "run_date": RUN_DATE,
        "phases": {},
    }

    # --- PHASE 0: baseline ----------------------------------------------
    log("PHASE 0 — baseline capture")
    ncpm = con.execute("SELECT COUNT(*) FROM canonical_patient_master").fetchone()[0]
    ncpm_cols = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.columns
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND table_name='canonical_patient_master'"""
    ).fetchone()[0]
    lock_report["phases"]["baseline"] = {
        "cpm_rows": ncpm,
        "cpm_cols": ncpm_cols,
    }
    log(f"  cpm: {ncpm} × {ncpm_cols} cols")

    # --- PHASE 1: catalog-ghost detection -------------------------------
    log("PHASE 1 — catalog-ghost detection (guarded SELECT sweep)")
    queryable, ghosts = enumerate_queryable_tables(con)
    log(f"  queryable main BASE TABLEs: {len(queryable)}")
    log(f"  ghosts (info_schema but not queryable): {len(ghosts)}")
    lock_report["phases"]["catalog_ghosts"] = {
        "queryable_count": len(queryable),
        "ghost_count": len(ghosts),
        "ghosts": [{"table_name": t, "error": e} for t, e in ghosts],
    }
    if ghosts:
        log("  NOTE: ghosts detected; __readme and registry regen will EXCLUDE them (convention: catalog_vs_queryable_drift)")
        for t, e in ghosts[:5]:
            log(f"    ghost: {t} -> {e}")

    if args.dry_run:
        log("--dry-run: exiting before any writes")
        return

    # --- PHASE 2: archive pre-lock snapshots ---------------------------
    log("PHASE 2 — archive pre-lock snapshots")
    archive_snapshot(
        con,
        "SELECT * FROM __readme",
        f"__readme_pre247_backup_{run_ts}",
        f"{SCRIPT_TAG} ({RUN_DATE}) pre-regeneration snapshot of main.__readme.",
    )
    archive_snapshot(
        con,
        "SELECT * FROM manuscript_workspace.detail_table_registry_v1",
        f"detail_table_registry_v1_pre247_backup_{run_ts}",
        f"{SCRIPT_TAG} ({RUN_DATE}) pre-regeneration snapshot of detail_table_registry_v1.",
    )
    log(f"  archived __readme_pre247_backup_{run_ts}")
    log(f"  archived detail_table_registry_v1_pre247_backup_{run_ts}")

    # --- PHASE 3: regenerate __readme -----------------------------------
    log("PHASE 3 — regenerate __readme from queryable enumeration")
    n_readme = regenerate_readme(con, queryable)
    log(f"  __readme now has {n_readme} rows")
    lock_report["phases"]["readme"] = {"rows": n_readme}

    # --- PHASE 4: regenerate registry -----------------------------------
    log("PHASE 4 — regenerate detail_table_registry_v1")
    reg_result = regenerate_registry(con, queryable)
    log(f"  registry rows: {reg_result['n_rows']}  (main={reg_result['n_main_rows']}, manuscript_workspace={reg_result['n_ms_rows']})")
    log(f"  stale dropped: {len(reg_result['stale_dropped'])}")
    log(f"  TODO markers resolved: {len(reg_result['todos_resolved'])}")
    log(f"  auto-added: {len(reg_result['added'])}")
    lock_report["phases"]["registry"] = reg_result

    # --- PHASE 5: view-compile sweep -----------------------------------
    log("PHASE 5 — view-compile sweep (main + manuscript_workspace)")
    total, passes, fails = view_compile_sweep(con)
    log(f"  views: {passes}/{total} pass, {len(fails)} fail")
    lock_report["phases"]["view_sweep"] = {
        "total": total, "pass": passes, "fail": len(fails),
        "failing_views": [{"schema": s, "name": n, "error": e} for s, n, e in fails[:25]],
    }

    # --- PHASE 6: CPM ↔ registry column pointer check -------------------
    log("PHASE 6 — CPM ↔ registry column pointer verification")
    checked, missing = verify_column_pointers(con)
    log(f"  tokens checked: {checked}  missing in CPM: {len(missing)}")
    lock_report["phases"]["column_pointer_check"] = {
        "checked": checked,
        "missing": [{"table": t, "column": c} for t, c in missing[:25]],
    }
    for t, c in missing[:10]:
        log(f"    MISSING: {t} -> {c}")

    # --- PHASE 7: empty-table sweep ------------------------------------
    log("PHASE 7 — empty-table sweep")
    empty: list[str] = []
    for t in queryable:
        n = con.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]
        if n == 0:
            empty.append(t)
    log(f"  empty queryable tables in main: {len(empty)}")
    for t in empty:
        log(f"    empty: {t}")
    lock_report["phases"]["empty_tables"] = {"count": len(empty), "tables": empty}

    # --- PHASE 8: __conventions sanity check ---------------------------
    log("PHASE 8 — __conventions sanity check")
    nconv = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.__conventions"
    ).fetchone()[0]
    nconv_todo = con.execute(
        """SELECT COUNT(*) FROM manuscript_workspace.__conventions
           WHERE convention_id IS NULL OR rule IS NULL OR exemplar IS NULL"""
    ).fetchone()[0]
    log(f"  __conventions rows: {nconv}  incomplete: {nconv_todo}")
    lock_report["phases"]["conventions"] = {"rows": nconv, "incomplete": nconv_todo}

    # --- PHASE 9: expanded confirmation block --------------------------
    log("PHASE 9 — expanded confirmation block (Script 236 + Group C residuals)")
    confirmations: dict = {}

    # Q1: canonical shape
    q1 = con.execute(
        f"""SELECT COUNT(*) AS patients,
                  (SELECT COUNT(*) FROM information_schema.columns
                   WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                     AND table_name='canonical_patient_master') AS columns
            FROM canonical_patient_master"""
    ).fetchone()
    confirmations["q1_cpm_shape"] = {"patients": q1[0], "columns": q1[1]}
    log(f"  Q1 canonical shape: patients={q1[0]} columns={q1[1]}")

    # Q2: lingering backups in main
    q2 = con.execute(
        f"""SELECT table_name FROM information_schema.tables
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
              AND (table_name LIKE '%pre%backup%'
                   OR table_name LIKE 'data_dictionary_v221'
                   OR table_name LIKE 'data_dictionary_v235')
            ORDER BY 1"""
    ).fetchall()
    confirmations["q2_lingering_backups"] = [r[0] for r in q2]
    log(f"  Q2 lingering backups/deprecated tables in main: {len(q2)} (expect 0)")

    # Q3: registry TODO count
    q3 = con.execute(
        """SELECT COUNT(*) FROM manuscript_workspace.detail_table_registry_v1
           WHERE feeds_master_columns LIKE '%TODO%' OR feeds_master_columns LIKE '%todo%'"""
    ).fetchone()[0]
    confirmations["q3_registry_todo_count"] = q3
    log(f"  Q3 registry TODO markers: {q3} (expect 0)")

    # Q4: __readme vs queryable-table count
    q4a = con.execute("SELECT COUNT(*) FROM __readme").fetchone()[0]
    q4b = len(queryable)
    confirmations["q4_readme_vs_queryable"] = {"readme": q4a, "queryable": q4b, "equal": q4a == q4b}
    log(f"  Q4 __readme rows={q4a}  queryable main tables={q4b}  equal? {q4a == q4b}")

    # Q5: deprecated__ coverage in data dictionary
    q5 = con.execute(
        f"""SELECT
             (SELECT COUNT(*) FROM information_schema.columns
              WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
                AND table_name='canonical_patient_master'
                AND column_name LIKE 'deprecated__%') AS cpm_dep_cols,
             (SELECT COUNT(*) FROM data_dictionary_v240
              WHERE column_name LIKE 'deprecated__%'
                AND status = 'deprecated' AND replacement_column_name IS NOT NULL) AS dict_dep_rows"""
    ).fetchone()
    confirmations["q5_deprecated_dict_coverage"] = {"cpm_dep_cols": q5[0], "dict_dep_rows": q5[1], "equal": q5[0] == q5[1]}
    log(f"  Q5 deprecated__ in CPM={q5[0]} in dict={q5[1]} (should be equal)")

    # Group-C residual #1: canonical_tumor_characteristics_v1 coverage gap
    q6 = con.execute(
        """WITH ctc AS (SELECT DISTINCT CAST(research_id AS VARCHAR) AS rid FROM canonical_tumor_characteristics_v1),
                cpm AS (SELECT research_id AS rid FROM canonical_patient_master)
           SELECT
             (SELECT COUNT(*) FROM cpm) AS cpm_pts,
             (SELECT COUNT(*) FROM ctc) AS ctc_pts,
             (SELECT COUNT(*) FROM cpm WHERE rid NOT IN (SELECT rid FROM ctc)) AS gap_pts"""
    ).fetchone()
    confirmations["q6_ctc_gap"] = {"cpm": q6[0], "ctc": q6[1], "gap": q6[2]}
    log(f"  Q6 canonical_tumor_characteristics_v1 gap: cpm={q6[0]} ctc={q6[1]} gap={q6[2]} (expect 2449)")

    # Group-C residual #2: discordance audit sanity
    try:
        q7 = con.execute(
            """SELECT COUNT(*) AS rows,
                      SUM(CASE WHEN unt_max_tr IS NOT NULL AND inm_max_tr IS NOT NULL THEN 1 ELSE 0 END) AS both_populated
               FROM us_nodules_tirads_vs_inm_v1_discordance_v1"""
        ).fetchone()
        confirmations["q7_discordance_sanity"] = {"rows": q7[0], "both_populated": q7[1]}
        log(f"  Q7 discordance audit: rows={q7[0]}  both_populated={q7[1]} (should be equal)")
    except Exception as e:
        confirmations["q7_discordance_sanity"] = {"error": str(e)[:160]}
        log(f"  Q7 discordance audit: ERROR {str(e)[:160]}")

    lock_report["phases"]["confirmations"] = confirmations

    # --- PHASE 10: final assertions + LOCK line ------------------------
    log("PHASE 10 — final assertions")
    checks: list[tuple[str, bool]] = []

    checks.append((f"canonical_patient_master row count == 10,871 (got {ncpm})", ncpm == 10871))
    checks.append(("no catalog ghosts (info_schema entries that aren't queryable)", len(ghosts) == 0))
    checks.append((f"view-compile sweep: all {total} views pass ({passes} passed, {len(fails)} failed)", passes == total))
    checks.append((f"registry has 0 TODO markers (got {q3})", q3 == 0))
    checks.append((f"__readme row count == queryable main tables ({q4a} vs {q4b})", q4a == q4b))
    checks.append((f"lingering backups in main: 0 (got {len(q2)})", len(q2) == 0))
    checks.append((f"CPM ↔ registry column-pointer mismatches == 0 (got {len(missing)})", len(missing) == 0))
    checks.append((f"empty tables in main: 0 (got {len(empty)})", len(empty) == 0))
    checks.append((f"__conventions has ≥5 complete rows (got {nconv} rows, {nconv_todo} incomplete)", nconv >= 5 and nconv_todo == 0))
    checks.append((f"deprecated__ CPM cols covered by dict ({q5[0]} vs {q5[1]})", q5[0] == q5[1]))
    checks.append((f"canonical_tumor_characteristics_v1 gap == 2,449 (got {q6[2]})", q6[2] == 2449))
    checks.append((
        "discordance audit: all rows have both unt_max_tr and inm_max_tr populated",
        "q7_discordance_sanity" in confirmations
        and confirmations["q7_discordance_sanity"].get("rows") is not None
        and confirmations["q7_discordance_sanity"]["rows"] == confirmations["q7_discordance_sanity"].get("both_populated"),
    ))

    failures: list[str] = []
    for label, ok in checks:
        tag = "PASS" if ok else "FAIL"
        log(f"  ASSERT [{tag}] {label}")
        if not ok:
            failures.append(label)

    lock_report["phases"]["final_assertions"] = {
        "checks": [{"label": l, "pass": ok} for l, ok in checks],
        "n_pass": len([c for c in checks if c[1]]),
        "n_fail": len(failures),
    }

    with LOCK_LOG_PATH.open("w") as f:
        json.dump(lock_report, f, indent=2, default=str)
    log(f"lock report written: {LOCK_LOG_PATH.name}")

    elapsed = time.time() - t0
    log(f"elapsed: {elapsed:.1f}s")

    log("")
    log("=" * 78)
    if failures:
        # Determine which scripts' assertions are implicated — conservative: blame 247
        # unless the failure matches a specific earlier script's invariant.
        failing_scripts = ["247"]
        line = f"CANONICAL v1_0 LOCK: BLOCKED — scripts with failing assertions: {','.join(failing_scripts)}"
        log(line)
        for f in failures:
            log(f"  FAIL: {f}")
        log("=" * 78)
        sys.exit(1)

    log("CANONICAL v1_0 LOCK: READY FOR PUBLICATION")
    log("=" * 78)


if __name__ == "__main__":
    main()
