#!/usr/bin/env python3
"""
Script 268 - Lock Bethesda semantics convention and rebuild bethesda_final
              + era columns + per-FNA derivation audit + index-nodule best-effort.

Convention (locked):
  bethesda_semantics = preop_worst_calculated_from_morphology_era_preserved

Derivation:
  bethesda_calculated =
    fna_cytology.category_num               when BETWEEN 1 AND 6 (morphology)
    TRY_CAST(original_bethesda AS INT)      when BETWEEN 1 AND 6 (number-only fallback)
    NULL                                    otherwise

  bethesda_final = MAX(bethesda_calculated) across preop FNAs per patient
                   (preop = fna_date_parsed < first_surgery_date)

Date parsing: multi-format COALESCE chain (ISO + M/D/YYYY + 2-digit-year),
              with fna_episode_master_v2.resolved_fna_date as fallback.

Era-specific columns (parallel):
  bethesda_max_preop_2010, bethesda_max_preop_2015, bethesda_max_preop_2023

Audit columns:
  n_bethesda_calculated_fnas, n_bethesda_number_only_fnas,
  bethesda_derivation_methods (pipe-joined distinct methods)

Index-nodule (best-effort, low coherence expected):
  bethesda_index_nodule, bethesda_index_nodule_linkage_source

Outputs:
  scripts/output/268_run.log
  scripts/output/268_prior_bethesda_final_distribution.json
  scripts/output/268_bethesda_distribution.md
  scripts/output/268_undated_fnas.json
  scripts/output/268_decision_log.json
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

RUN_LOG = OUT_DIR / "268_run.log"
PRIOR_DIST = OUT_DIR / "268_prior_bethesda_final_distribution.json"
DIST_REPORT = OUT_DIR / "268_bethesda_distribution.md"
UNDATED_LOG = OUT_DIR / "268_undated_fnas.json"
DECISION_LOG = OUT_DIR / "268_decision_log.json"

SCRIPT_TAG = "Script 268"
SCRIPT_NUM = "268"
RUN_DATE = "2026-04-17"

CPM = "canonical_patient_master"
ROLLUP = "main._bethesda_preop_rollup_268"
INDEX_NODULE = "main._bethesda_index_nodule_268"
CONVENTIONS = f"{PUBLICATION_DB}.manuscript_workspace.__conventions"
LCS = f"{PUBLICATION_DB}.manuscript_workspace.legacy_column_sweep_v1_1"
REGISTRY = f"{PUBLICATION_DB}.manuscript_workspace.detail_table_registry_v1"
POINTER = f"{PUBLICATION_DB}.manuscript_workspace.canonical_detail_pointer_v1"

CONV_KEY = "bethesda_semantics"
CONV_EXEMPLAR = "preop_worst_calculated_from_morphology_era_preserved"
CONV_RULE = (
    "bethesda_final = MAX(bethesda_calculated) across preop FNAs per patient. "
    "bethesda_calculated = fna_cytology.category_num when BETWEEN 1 AND 6 "
    "(morphology-derived via rules/LLM on path_text); else "
    "TRY_CAST(original_bethesda AS INT) when BETWEEN 1 AND 6; else NULL. "
    "Era-specific max columns (bethesda_max_preop_2010/2015/2023) parallel "
    "this using bethesda_YYYY_num from the same extraction. Per-FNA derivation "
    "method preserved and rolled up at patient level. Most-recent-preop and "
    "composite semantics rejected as clinically irrelevant. Index-nodule kept "
    "as low-coherence parallel column."
)

# Multi-format date parser snippet (shared across CTEs)
DATE_PARSE_SQL = """
COALESCE(
  TRY_CAST(fc.fna_date AS DATE),
  TRY_STRPTIME(fc.fna_date, '%-m/%-d/%Y')::DATE,
  TRY_STRPTIME(fc.fna_date, '%m/%d/%Y')::DATE,
  TRY_STRPTIME(fc.fna_date, '%-m/%-d/%y')::DATE,
  TRY_STRPTIME(fc.fna_date, '%m/%d/%y')::DATE
)
""".strip()

NEW_BETHESDA_COLS_PRIMARY = [
    "bethesda_final",
    "bethesda_max_preop_2010",
    "bethesda_max_preop_2015",
    "bethesda_max_preop_2023",
    "n_bethesda_calculated_fnas",
    "n_bethesda_number_only_fnas",
    "bethesda_derivation_methods",
]

INDEX_COLS_PRIMARY = [
    "bethesda_index_nodule",
    "bethesda_index_nodule_linkage_source",
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


def schema_verify(con, log) -> None:
    log("\n--- SCHEMA VERIFICATION ---")
    expected = {"category_num", "method", "path_text", "original_bethesda",
                "bethesda_2010_num", "bethesda_2015_num", "bethesda_2023_num",
                "fna_date", "research_id", "fna_index"}
    have = {r[0] for r in con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='fna_cytology'
    """).fetchall()}
    missing = expected - have
    if missing:
        raise SystemExit(f"FAIL fna_cytology missing expected columns: {sorted(missing)}")
    log(f"  pass: fna_cytology has all {len(expected)} expected columns")

    n_rows = con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0]
    n_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
    """).fetchone()[0]
    log(f"  CPM starting state: {n_rows} rows x {n_cols} cols")
    if n_rows != 10871:
        raise SystemExit(f"PREFLIGHT FAIL: CPM rows={n_rows}, expected 10871")
    # Accept fresh (1491 after 267) or idempotent re-run (1499 after a prior 268 run)
    if n_cols not in (1491, 1499):
        raise SystemExit(
            f"PREFLIGHT FAIL: CPM cols={n_cols}, expected 1491 (fresh) or 1499 (re-run)")


def lock_convention(con, log) -> dict:
    log("\n--- LOCK BETHESDA SEMANTICS CONVENTION ---")
    existing = con.execute(
        f"SELECT convention_id, category, exemplar, rule, established_in "
        f"FROM {CONVENTIONS} WHERE convention_id = ?", [CONV_KEY]
    ).fetchall()
    if existing:
        row = existing[0]
        log(f"  existing convention row found: convention_id={row[0]} exemplar={row[2]!r}")
        if row[2] != CONV_EXEMPLAR:
            raise SystemExit(
                f"CONFLICT: __conventions has convention_id='{CONV_KEY}' with "
                f"exemplar={row[2]!r}, expected {CONV_EXEMPLAR!r}. "
                "Refusing to overwrite. Surface to user."
            )
        log("  exemplar matches expected value; idempotent no-op")
        return {"action": "noop_already_present", "row": list(row)}

    con.execute(
        f"""
        INSERT INTO {CONVENTIONS} (convention_id, category, rule, exemplar, established_in)
        VALUES (?, ?, ?, ?, ?)
        """,
        [CONV_KEY, "semantics", CONV_RULE, CONV_EXEMPLAR, "script_268"],
    )
    n = con.execute(
        f"SELECT COUNT(*) FROM {CONVENTIONS} WHERE convention_id = ?", [CONV_KEY]
    ).fetchone()[0]
    if n != 1:
        raise SystemExit(f"FAIL: after insert, expected 1 row, got {n}")
    log(f"  inserted convention row '{CONV_KEY}' = '{CONV_EXEMPLAR}'")
    return {"action": "inserted", "convention_id": CONV_KEY, "exemplar": CONV_EXEMPLAR}


def materialize_preop_rollup(con, log) -> dict:
    log("\n--- MATERIALIZE PREOP ROLLUP (multi-format date parse + fallback) ---")
    con.execute(f"DROP TABLE IF EXISTS {ROLLUP}")
    con.execute(f"""
        CREATE TABLE {ROLLUP} AS
        WITH fna_derived AS (
          SELECT
            fc.research_id,
            fc.fna_index,
            fc.method,
            {DATE_PARSE_SQL}                                       AS fna_date_parsed,
            CASE
              WHEN fc.category_num BETWEEN 1 AND 6
                THEN fc.category_num
              WHEN TRY_CAST(fc.original_bethesda AS INT) BETWEEN 1 AND 6
                THEN TRY_CAST(fc.original_bethesda AS INT)
              ELSE NULL
            END                                                    AS bethesda_calculated,
            CASE
              WHEN fc.category_num BETWEEN 1 AND 6
                THEN 'calculated_' || COALESCE(fc.method, 'unknown')
              WHEN TRY_CAST(fc.original_bethesda AS INT) BETWEEN 1 AND 6
                THEN 'number_only_fallback'
              ELSE 'unresolved'
            END                                                    AS bethesda_derivation_method,
            fc.bethesda_2010_num,
            fc.bethesda_2015_num,
            fc.bethesda_2023_num
          FROM fna_cytology fc
        ),
        fna_with_fallback_date AS (
          SELECT fd.*,
                 COALESCE(
                   fd.fna_date_parsed,
                   (SELECT MIN(fem.resolved_fna_date)
                    FROM fna_episode_master_v2 fem
                    WHERE fem.research_id = fd.research_id)
                 )                                                 AS fna_date_final
          FROM fna_derived fd
        ),
        preop_fnas AS (
          SELECT fwfd.*, cpm.first_surgery_date
          FROM fna_with_fallback_date fwfd
          JOIN {CPM} cpm ON fwfd.research_id = cpm.research_id
          WHERE fwfd.fna_date_final IS NOT NULL
            AND cpm.first_surgery_date IS NOT NULL
            AND fwfd.fna_date_final < cpm.first_surgery_date
            AND fwfd.bethesda_calculated IS NOT NULL
        )
        SELECT
          research_id,
          MAX(bethesda_calculated)                                 AS bethesda_final_new,
          MAX(bethesda_2010_num)                                   AS bethesda_max_preop_2010,
          MAX(bethesda_2015_num)                                   AS bethesda_max_preop_2015,
          MAX(bethesda_2023_num)                                   AS bethesda_max_preop_2023,
          COUNT(*) FILTER (WHERE bethesda_derivation_method LIKE 'calculated_%')
                                                                   AS n_bethesda_calculated_fnas,
          COUNT(*) FILTER (WHERE bethesda_derivation_method = 'number_only_fallback')
                                                                   AS n_bethesda_number_only_fnas,
          STRING_AGG(DISTINCT bethesda_derivation_method, '|'
                     ORDER BY bethesda_derivation_method)
                                                                   AS bethesda_derivation_methods
        FROM preop_fnas
        GROUP BY research_id
    """)
    n_rollup = con.execute(f"SELECT COUNT(*) FROM {ROLLUP}").fetchone()[0]
    log(f"  rollup rows: {n_rollup} patients with bethesda_final")

    # Coverage diagnostic
    diag = con.execute(f"""
        SELECT
          (SELECT COUNT(*) FROM fna_cytology)                      AS total_fnas,
          (SELECT COUNT(*) FROM fna_cytology fc
           WHERE {DATE_PARSE_SQL.replace('fc.fna_date', 'fc.fna_date')}
             IS NOT NULL)                                          AS dated_fnas,
          (SELECT COUNT(*) FROM fna_cytology fc
           WHERE COALESCE(
             TRY_CAST(fc.fna_date AS DATE),
             TRY_STRPTIME(fc.fna_date, '%-m/%-d/%Y')::DATE,
             TRY_STRPTIME(fc.fna_date, '%m/%d/%Y')::DATE,
             TRY_STRPTIME(fc.fna_date, '%-m/%-d/%y')::DATE,
             TRY_STRPTIME(fc.fna_date, '%m/%d/%y')::DATE
           ) IS NULL)                                              AS still_undated
    """).fetchone()
    log(f"  date parse coverage: total={diag[0]} dated={diag[1]} still_undated={diag[2]}")

    # Surface unresolved + truly undated detail
    undated_samples = con.execute("""
        SELECT fc.research_id, fc.fna_index, fc.fna_date,
               fc.category_num, fc.original_bethesda
        FROM fna_cytology fc
        WHERE COALESCE(
          TRY_CAST(fc.fna_date AS DATE),
          TRY_STRPTIME(fc.fna_date, '%-m/%-d/%Y')::DATE,
          TRY_STRPTIME(fc.fna_date, '%m/%d/%Y')::DATE,
          TRY_STRPTIME(fc.fna_date, '%-m/%-d/%y')::DATE,
          TRY_STRPTIME(fc.fna_date, '%m/%d/%y')::DATE
        ) IS NULL
        ORDER BY fc.research_id, fc.fna_index
        LIMIT 200
    """).fetchall()
    UNDATED_LOG.write_text(json.dumps({
        "n_still_undated": int(diag[2]),
        "samples_first_200": [
            {"research_id": r[0], "fna_index": r[1], "fna_date": r[2],
             "category_num": r[3], "original_bethesda": r[4]}
            for r in undated_samples
        ],
    }, indent=2, default=str))
    log(f"  wrote {UNDATED_LOG}")

    if int(diag[2]) > 100:
        log(f"  WARNING: still_undated={diag[2]} > 100 - data quality concern but not aborting "
            "(per addendum: only abort if >100 AND it materially affects coverage)")

    # No 'unresolved' should leak into rollup since we filter bethesda_calculated IS NOT NULL
    n_unresolved_pts = con.execute(f"""
        SELECT COUNT(*) FROM {ROLLUP}
        WHERE bethesda_derivation_methods ILIKE '%unresolved%'
    """).fetchone()[0]
    if n_unresolved_pts > 0:
        raise SystemExit(
            f"FAIL: {n_unresolved_pts} patients leaked 'unresolved' into "
            "bethesda_derivation_methods. Bug in rollup CTE."
        )
    log("  pass: 0 patients with 'unresolved' in derivation_methods")

    return {
        "rollup_rows": int(n_rollup),
        "total_fnas": int(diag[0]),
        "dated_fnas": int(diag[1]),
        "still_undated": int(diag[2]),
    }


def materialize_index_nodule(con, log) -> dict:
    """
    Best-effort index-nodule Bethesda via three linkage chains (priority order):
      (a) specimen_tumor_focus_v1 (gives index tumor's surgery date) -> nearest
          preop FNA on same patient -> fna_cytology
      (b) imaging_fna_linkage_v3 (gives FNA -> imaging-nodule link) -> primary
          nodule (smallest nodule_id per patient as proxy) -> fna_cytology
      (c) fna_episode_master_v2 -> fna_cytology direct (per patient: take FNA
          with latest preop date as 'index' proxy)

    research_id types vary; CAST AS VARCHAR for joins.
    """
    log("\n--- MATERIALIZE INDEX-NODULE BETHESDA (best-effort) ---")
    con.execute(f"DROP TABLE IF EXISTS {INDEX_NODULE}")

    con.execute(f"""
        CREATE TABLE {INDEX_NODULE} AS
        WITH fna_derived AS (
          SELECT
            CAST(fc.research_id AS VARCHAR)                        AS research_id_v,
            fc.fna_index,
            {DATE_PARSE_SQL}                                       AS fna_date_parsed,
            CASE
              WHEN fc.category_num BETWEEN 1 AND 6
                THEN fc.category_num
              WHEN TRY_CAST(fc.original_bethesda AS INT) BETWEEN 1 AND 6
                THEN TRY_CAST(fc.original_bethesda AS INT)
              ELSE NULL
            END                                                    AS bethesda_calculated
          FROM fna_cytology fc
        ),
        -- Linkage source (a): index tumor focus -> nearest preop FNA
        focus_index AS (
          SELECT
            CAST(stf.research_id AS VARCHAR)                       AS rid,
            MIN(stf.surg_date_canonical)                           AS index_surg_date
          FROM specimen_tumor_focus_v1 stf
          WHERE stf.tumor_ordinal = 1 OR stf.tumor_index = 0
          GROUP BY 1
        ),
        a_link AS (
          SELECT
            fi.rid                                                 AS research_id,
            'specimen_tumor_focus_v1'                              AS linkage_source,
            (
              SELECT fd.bethesda_calculated
              FROM fna_derived fd
              WHERE fd.research_id_v = fi.rid
                AND fd.bethesda_calculated IS NOT NULL
                AND fd.fna_date_parsed IS NOT NULL
                AND fd.fna_date_parsed < fi.index_surg_date
              ORDER BY fd.fna_date_parsed DESC
              LIMIT 1
            )                                                      AS bethesda_index_nodule
          FROM focus_index fi
        ),
        -- Linkage source (b): imaging_fna_linkage_v3 -> primary nodule per patient
        primary_nodule AS (
          SELECT
            CAST(ifl.research_id AS VARCHAR)                       AS rid,
            (ARRAY_AGG(ifl.nodule_id ORDER BY ifl.nodule_id))[1]   AS primary_nodule_id
          FROM imaging_fna_linkage_v3 ifl
          WHERE ifl.analysis_eligible_link_flag = TRUE
          GROUP BY 1
        ),
        b_link AS (
          SELECT
            pn.rid                                                 AS research_id,
            'imaging_fna_linkage_v3'                               AS linkage_source,
            (
              SELECT fd.bethesda_calculated
              FROM imaging_fna_linkage_v3 ifl2
              JOIN fna_episode_master_v2 fem
                ON CAST(fem.research_id AS VARCHAR) = pn.rid
                AND fem.fna_episode_id = ifl2.fna_episode_id
              JOIN fna_derived fd
                ON fd.research_id_v = pn.rid
                AND fd.fna_index    = fem.fna_episode_id
              WHERE CAST(ifl2.research_id AS VARCHAR) = pn.rid
                AND ifl2.nodule_id = pn.primary_nodule_id
                AND fd.bethesda_calculated IS NOT NULL
              ORDER BY ifl2.linkage_score DESC
              LIMIT 1
            )                                                      AS bethesda_index_nodule
          FROM primary_nodule pn
        ),
        -- Linkage source (c): fna_episode_master_v2 -> direct, take latest preop FNA per patient
        c_link AS (
          SELECT
            CAST(fem.research_id AS VARCHAR)                       AS research_id,
            'fna_episode_direct'                                   AS linkage_source,
            (
              SELECT fd.bethesda_calculated
              FROM fna_derived fd
              JOIN {CPM} cpm
                ON fd.research_id_v = cpm.research_id
              WHERE fd.research_id_v = CAST(fem.research_id AS VARCHAR)
                AND fd.bethesda_calculated IS NOT NULL
                AND fd.fna_date_parsed IS NOT NULL
                AND cpm.first_surgery_date IS NOT NULL
                AND fd.fna_date_parsed < cpm.first_surgery_date
              ORDER BY fd.fna_date_parsed DESC
              LIMIT 1
            )                                                      AS bethesda_index_nodule
          FROM fna_episode_master_v2 fem
          GROUP BY 1
        ),
        merged AS (
          SELECT cpm.research_id,
                 a.bethesda_index_nodule       AS a_val,
                 b.bethesda_index_nodule       AS b_val,
                 c.bethesda_index_nodule       AS c_val
          FROM {CPM} cpm
          LEFT JOIN a_link a ON a.research_id = cpm.research_id
          LEFT JOIN b_link b ON b.research_id = cpm.research_id
          LEFT JOIN c_link c ON c.research_id = cpm.research_id
        )
        SELECT
          research_id,
          COALESCE(a_val, b_val, c_val)                            AS bethesda_index_nodule,
          CASE
            WHEN a_val IS NOT NULL THEN 'specimen_tumor_focus_v1'
            WHEN b_val IS NOT NULL THEN 'imaging_fna_linkage_v3'
            WHEN c_val IS NOT NULL THEN 'fna_episode_direct'
            ELSE NULL
          END                                                      AS bethesda_index_nodule_linkage_source
        FROM merged
        WHERE COALESCE(a_val, b_val, c_val) IS NOT NULL
    """)

    n = con.execute(f"SELECT COUNT(*) FROM {INDEX_NODULE}").fetchone()[0]
    log(f"  index-nodule rollup rows: {n}")

    by_src = con.execute(f"""
        SELECT bethesda_index_nodule_linkage_source, COUNT(*)
        FROM {INDEX_NODULE}
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    log("  coverage by linkage_source:")
    for src, c in by_src:
        log(f"    {src!r}: {c}")

    return {
        "index_nodule_rows": int(n),
        "by_linkage_source": [(s, int(c)) for s, c in by_src],
    }


def log_prior_bethesda_distribution(con, log) -> dict:
    log("\n--- LOG PRIOR bethesda_final DISTRIBUTION ---")
    if PRIOR_DIST.exists():
        existing = json.loads(PRIOR_DIST.read_text())
        if "prior_bethesda_final" in existing:
            dist = existing["prior_bethesda_final"]
            log(f"  prior distribution loaded from existing {PRIOR_DIST.name}: {dist}")
        else:
            dist = {}
    else:
        rows = con.execute(f"""
            SELECT bethesda_final, COUNT(*)
            FROM {CPM} GROUP BY 1 ORDER BY 1 NULLS LAST
        """).fetchall()
        dist = {("NULL" if v is None else str(v)): int(n) for v, n in rows}
        PRIOR_DIST.write_text(json.dumps({"prior_bethesda_final": dist}, indent=2))
        log(f"  prior distribution: {dist}")

    now_ts = con.execute("SELECT current_timestamp").fetchone()[0]
    con.execute(f"DELETE FROM {LCS} WHERE column_name = 'bethesda_final'")
    con.execute(
        f"""
        INSERT INTO {LCS}
          (column_name, version, stem, max_version_in_cpm, successor_column, inventoried_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        ["bethesda_final", None, "bethesda_final", None,
         "bethesda_final (rebuilt under preop_worst_calculated_from_morphology_era_preserved, "
         "Script 268 on 2026-04-17 - prior source extracted_fna_bethesda_v1)", now_ts],
    )
    log("  wrote sweep row for prior bethesda_final")

    return dist


def rebuild_cpm(con, log) -> tuple[int, int]:
    log("\n--- REBUILD canonical_patient_master ---")
    n_cols_pre = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
    """).fetchone()[0]
    log(f"  pre-rebuild cols: {n_cols_pre}")

    # EXCLUDE all 9 new cols if present (idempotent re-run safety) + bethesda_final.
    cpm_existing = {r[0] for r in con.execute(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
    """).fetchall()}
    excludes = ["bethesda_final"] + [
        c for c in (
            "bethesda_max_preop_2010", "bethesda_max_preop_2015",
            "bethesda_max_preop_2023", "n_bethesda_calculated_fnas",
            "n_bethesda_number_only_fnas", "bethesda_derivation_methods",
            "bethesda_index_nodule", "bethesda_index_nodule_linkage_source",
        ) if c in cpm_existing
    ]
    excludes_sql = ", ".join(excludes)
    log(f"  CTAS will EXCLUDE: {excludes}")

    con.execute(f"""
        CREATE OR REPLACE TABLE {CPM} AS
        SELECT
          cpm.* EXCLUDE ({excludes_sql}),
          bpr.bethesda_final_new                AS bethesda_final,
          bpr.bethesda_max_preop_2010,
          bpr.bethesda_max_preop_2015,
          bpr.bethesda_max_preop_2023,
          bpr.n_bethesda_calculated_fnas,
          bpr.n_bethesda_number_only_fnas,
          bpr.bethesda_derivation_methods,
          bin.bethesda_index_nodule,
          bin.bethesda_index_nodule_linkage_source
        FROM {CPM} cpm
        LEFT JOIN {ROLLUP}       bpr ON bpr.research_id = cpm.research_id
        LEFT JOIN {INDEX_NODULE} bin ON bin.research_id = cpm.research_id
    """)
    n_rows = con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0]
    n_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
    """).fetchone()[0]
    log(f"  post-rebuild CPM: {n_rows} rows x {n_cols} cols")

    if n_rows != 10871:
        raise SystemExit(f"FAIL spine: rows={n_rows} != 10871")
    # Net delta: 1,491 - 1 (excluded bethesda_final) + 9 (added incl. rebuilt) = 1,499
    expected_cols = 1499
    if n_cols != expected_cols:
        raise SystemExit(f"FAIL col count: {n_cols} != {expected_cols}")
    log(f"  pass: rows=10871, cols={expected_cols}")

    return int(n_rows), int(n_cols)


def update_registry_pins(con, log) -> dict:
    log("\n--- UPDATE detail_table_registry_v1 PINS ---")
    info: dict = {"actions": []}

    # 1. Strip bethesda_final + bethesda_category from extracted_fna_bethesda_v1's
    #    feeds_normalized list, since fna_cytology now owns bethesda_final.
    cur = con.execute(
        f"SELECT feeds_master_columns_normalized FROM {REGISTRY} "
        f"WHERE detail_table_name = 'extracted_fna_bethesda_v1'"
    ).fetchone()
    if cur and cur[0]:
        existing = [t for t in cur[0].split(";") if t]
        kept = [t for t in existing if t != "bethesda_final"]
        new_norm = ";".join(sorted(set(kept))) if kept else None
        con.execute(
            f"UPDATE {REGISTRY} SET feeds_master_columns_normalized = ? "
            f"WHERE detail_table_name = 'extracted_fna_bethesda_v1'",
            [new_norm],
        )
        info["actions"].append({
            "table": "extracted_fna_bethesda_v1",
            "removed_from_normalized": ["bethesda_final"],
            "new_normalized": new_norm,
        })
        log(f"  extracted_fna_bethesda_v1 normalized was {existing} -> {new_norm}")

    # 2. Pin all 7 primary cols + 2 index cols to fna_cytology (CTAS-merge)
    cur = con.execute(
        f"SELECT feeds_master_columns_normalized, feeds_master_columns_secondary "
        f"FROM {REGISTRY} WHERE detail_table_name = 'fna_cytology'"
    ).fetchone()
    if not cur:
        # Insert new registry row
        n_rows_fna = con.execute("SELECT COUNT(*) FROM fna_cytology").fetchone()[0]
        n_pts_fna = con.execute(
            "SELECT COUNT(DISTINCT research_id) FROM fna_cytology"
        ).fetchone()[0]
        feeds_str = ";".join(sorted(NEW_BETHESDA_COLS_PRIMARY))
        con.execute(
            f"""
            INSERT INTO {REGISTRY}
              (detail_table_name, schema_name, join_key, grain, total_rows,
               total_patients, domain, feeds_master_columns, description,
               canonical_version, feeds_master_columns_normalized,
               feeds_master_columns_secondary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ["fna_cytology", "main", "research_id",
             "one row per FNA cytology record",
             int(n_rows_fna), int(n_pts_fna), "pathology_cytology",
             "Per-FNA cytology records with morphology-calculated Bethesda "
             "(category_num). Single authoritative feeder for bethesda_* "
             "columns on canonical_patient_master under "
             "preop_worst_calculated_from_morphology_era_preserved.",
             "Script 268 (v1_0)", feeds_str, None],
        )
        info["actions"].append({"table": "fna_cytology", "action": "INSERTED",
                                "normalized": feeds_str})
        log(f"  inserted fna_cytology registry row with normalized={feeds_str}")
    else:
        existing_norm = set(t for t in (cur[0] or "").split(";") if t)
        existing_norm.update(NEW_BETHESDA_COLS_PRIMARY)
        new_norm = ";".join(sorted(existing_norm))
        con.execute(
            f"UPDATE {REGISTRY} "
            f"SET feeds_master_columns_normalized = ?, "
            f"description = ?, canonical_version = ? "
            f"WHERE detail_table_name = 'fna_cytology'",
            [new_norm,
             "Per-FNA cytology records with morphology-calculated Bethesda "
             "(category_num). Single authoritative feeder for bethesda_* "
             "columns on canonical_patient_master under "
             "preop_worst_calculated_from_morphology_era_preserved (Script 268).",
             "Script 268 (v1_0)"],
        )
        info["actions"].append({"table": "fna_cytology", "action": "UPDATED",
                                "normalized": new_norm})
        log(f"  updated fna_cytology normalized -> {new_norm}")

    # 3. Pin the 2 index-nodule cols to fna_episode_master_v2 (composite primary)
    cur = con.execute(
        f"SELECT feeds_master_columns_normalized FROM {REGISTRY} "
        f"WHERE detail_table_name = 'fna_episode_master_v2'"
    ).fetchone()
    if cur:
        existing_norm = set(t for t in (cur[0] or "").split(";") if t)
        existing_norm.update(INDEX_COLS_PRIMARY)
        new_norm = ";".join(sorted(existing_norm))
        con.execute(
            f"UPDATE {REGISTRY} SET feeds_master_columns_normalized = ? "
            f"WHERE detail_table_name = 'fna_episode_master_v2'",
            [new_norm],
        )
        info["actions"].append({"table": "fna_episode_master_v2",
                                "action": "UPDATED", "normalized": new_norm})
        log(f"  updated fna_episode_master_v2 normalized -> {new_norm}")

    # 4. Add 'bethesda_index_nodule' to secondary on the linkage tables
    for tbl in ("specimen_tumor_focus_v1", "imaging_fna_linkage_v3"):
        cur = con.execute(
            f"SELECT feeds_master_columns_secondary FROM {REGISTRY} "
            f"WHERE detail_table_name = ?", [tbl]
        ).fetchone()
        if not cur:
            log(f"  WARN: registry has no row for {tbl}; skipping secondary pin")
            continue
        existing_sec = set(t for t in (cur[0] or "").split(";") if t)
        existing_sec.add("bethesda_index_nodule")
        new_sec = ";".join(sorted(existing_sec))
        con.execute(
            f"UPDATE {REGISTRY} SET feeds_master_columns_secondary = ? "
            f"WHERE detail_table_name = ?", [new_sec, tbl]
        )
        info["actions"].append({"table": tbl, "action": "UPDATED_SECONDARY",
                                "secondary": new_sec})
        log(f"  updated {tbl} secondary -> {new_sec}")

    # 5. Verify pointer view: each new col has exactly 1 authoritative feeder
    log("\n  Verifying canonical_detail_pointer_v1 (view) reflects new pins:")
    target_cols = NEW_BETHESDA_COLS_PRIMARY + INDEX_COLS_PRIMARY
    targets_str = ",".join(repr(c) for c in target_cols)
    rows = con.execute(f"""
        SELECT master_column, COUNT(*) AS n_feeders,
               STRING_AGG(detail_table_name, '|') AS feeders
        FROM {POINTER}
        WHERE master_column IN ({targets_str})
        GROUP BY master_column
        ORDER BY master_column
    """).fetchall()
    pointer_status = {}
    fail = []
    for col in target_cols:
        match = next((r for r in rows if r[0] == col), None)
        if not match:
            pointer_status[col] = {"n_feeders": 0, "feeders": None}
            fail.append(f"{col}: 0 feeders")
        else:
            pointer_status[col] = {"n_feeders": int(match[1]), "feeders": match[2]}
            if int(match[1]) != 1:
                fail.append(f"{col}: {match[1]} feeders ({match[2]})")
    for col, st in pointer_status.items():
        log(f"    {col}: {st}")

    if fail:
        raise SystemExit(
            "FAIL: pointer view shows non-1 authoritative feeders for "
            f"{fail}. Registry pinning is broken."
        )
    log("  pass: every Bethesda column has exactly 1 authoritative feeder")

    info["pointer_status"] = pointer_status
    return info


def write_distribution_report(con, log, prior_dist: dict, rollup_info: dict,
                              index_info: dict) -> None:
    log("\n--- WRITE DISTRIBUTION REPORT ---")

    new_dist = con.execute(f"""
        SELECT bethesda_final, COUNT(*)
        FROM {CPM} GROUP BY 1 ORDER BY 1 NULLS LAST
    """).fetchall()
    new_dist_d = {("NULL" if v is None else str(v)): int(n) for v, n in new_dist}

    era = {}
    for col in ("bethesda_max_preop_2010", "bethesda_max_preop_2015",
                "bethesda_max_preop_2023"):
        rows = con.execute(
            f"SELECT {col}, COUNT(*) FROM {CPM} GROUP BY 1 ORDER BY 1 NULLS LAST"
        ).fetchall()
        era[col] = {("NULL" if v is None else str(v)): int(n) for v, n in rows}

    methods = con.execute(f"""
        SELECT bethesda_derivation_methods, COUNT(*)
        FROM {CPM} GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    methods_l = [(("NULL" if m is None else m), int(n)) for m, n in methods]

    num_only_dist = con.execute(f"""
        SELECT n_bethesda_number_only_fnas, COUNT(*)
        FROM {CPM} GROUP BY 1 ORDER BY 1 NULLS LAST
    """).fetchall()

    idx_src = con.execute(f"""
        SELECT bethesda_index_nodule_linkage_source, COUNT(*)
        FROM {CPM}
        WHERE bethesda_index_nodule IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()

    delta = []
    all_keys = sorted(set(prior_dist) | set(new_dist_d), key=lambda x: (x == "NULL", x))
    for k in all_keys:
        delta.append((k, prior_dist.get(k, 0), new_dist_d.get(k, 0)))

    n_unresolved = con.execute(f"""
        SELECT COUNT(*) FROM {CPM}
        WHERE bethesda_derivation_methods ILIKE '%unresolved%'
    """).fetchone()[0]

    pure_calculated_pts = con.execute(f"""
        SELECT COUNT(*) FROM {CPM}
        WHERE bethesda_final IS NOT NULL
          AND bethesda_derivation_methods NOT ILIKE '%number_only_fallback%'
    """).fetchone()[0]
    n_with_final = con.execute(
        f"SELECT COUNT(*) FROM {CPM} WHERE bethesda_final IS NOT NULL"
    ).fetchone()[0]

    md_lines = [
        "# Script 268 - Bethesda Distribution Report",
        f"_Generated {utc_now()}_",
        "",
        "## Convention locked",
        f"`bethesda_semantics = {CONV_EXEMPLAR}`",
        "",
        "## Date parse coverage",
        f"- Total FNAs: {rollup_info['total_fnas']}",
        f"- Dated (multi-format COALESCE): {rollup_info['dated_fnas']} "
        f"({100.0 * rollup_info['dated_fnas'] / rollup_info['total_fnas']:.2f}%)",
        f"- Still undated: {rollup_info['still_undated']}",
        f"- Patients in preop rollup: {rollup_info['rollup_rows']}",
        "",
        "## bethesda_final distribution (new vs prior)",
        "| value | prior_n | new_n | delta |",
        "|---|---:|---:|---:|",
    ]
    for k, p, n in delta:
        md_lines.append(f"| {k} | {p} | {n} | {n - p:+d} |")
    md_lines += ["", "## Era-specific distributions"]
    for col, d in era.items():
        md_lines.append(f"\n### {col}")
        md_lines.append("| value | n |")
        md_lines.append("|---|---:|")
        for k, n in d.items():
            md_lines.append(f"| {k} | {n} |")
    md_lines += [
        "",
        "## bethesda_derivation_methods value counts (across patients)",
        "| methods | n_patients |",
        "|---|---:|",
    ]
    for m, n in methods_l:
        md_lines.append(f"| {m} | {n} |")

    md_lines += [
        "",
        "## n_bethesda_number_only_fnas distribution",
        "| n_number_only_fnas | n_patients |",
        "|---:|---:|",
    ]
    for v, n in num_only_dist:
        v_str = "NULL" if v is None else v
        md_lines.append(f"| {v_str} | {n} |")

    md_lines += [
        "",
        "## Index-nodule coverage by linkage_source",
        "| linkage_source | n_patients |",
        "|---|---:|",
    ]
    for s, n in idx_src:
        s_str = "NULL" if s is None else s
        md_lines.append(f"| {s_str} | {n} |")
    md_lines.append(
        f"\nIndex-nodule total coverage: "
        f"{sum(int(c) for _, c in idx_src)}/{n_with_final} "
        f"({100.0 * sum(int(c) for _, c in idx_src) / max(n_with_final, 1):.1f}% of "
        "patients with bethesda_final)"
    )

    md_lines += [
        "",
        "## Pre-flight assertions",
        f"- Patients with bethesda_final: {n_with_final}",
        f"- Pure-calculated patients (no number_only fallback): {pure_calculated_pts}",
        f"- Patients with 'unresolved' in derivation_methods: {n_unresolved} "
        "(expected 0)",
    ]

    DIST_REPORT.write_text("\n".join(md_lines))
    log(f"  wrote {DIST_REPORT}")


def drop_staging(con, log) -> None:
    log("\n--- DROP STAGING TABLES ---")
    con.execute(f"DROP TABLE IF EXISTS {ROLLUP}")
    con.execute(f"DROP TABLE IF EXISTS {INDEX_NODULE}")
    log(f"  dropped {ROLLUP} and {INDEX_NODULE}")


def final_assertions(con, log) -> None:
    log("\n--- FINAL ASSERTIONS ---")
    n_rows = con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0]
    n_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
    """).fetchone()[0]
    if n_rows != 10871:
        raise SystemExit(f"FAIL: rows={n_rows} != 10871")
    # Net delta: -1 (excluded bethesda_final) + 9 added (incl. rebuilt bethesda_final)
    # = +8 net. 1,491 -> 1,499. The prompt's stated 1,500 double-counted bethesda_final.
    if n_cols != 1499:
        raise SystemExit(f"FAIL: cols={n_cols} != 1499 (1,491 - 1 excluded + 9 added)")
    log("  pass: CPM 10,871 x 1,499 cols")

    n_conv = con.execute(
        f"SELECT COUNT(*) FROM {CONVENTIONS} "
        f"WHERE convention_id = ? AND exemplar = ?",
        [CONV_KEY, CONV_EXEMPLAR],
    ).fetchone()[0]
    if n_conv != 1:
        raise SystemExit(f"FAIL: __conventions has {n_conv} rows for {CONV_KEY}, expected 1")
    log(f"  pass: __conventions has exactly 1 bethesda_semantics row")

    n_pointer = con.execute(
        f"SELECT COUNT(*) FROM {POINTER} WHERE master_column = 'bethesda_final'"
    ).fetchone()[0]
    if n_pointer != 1:
        raise SystemExit(
            f"FAIL: canonical_detail_pointer_v1 has {n_pointer} feeders for "
            "bethesda_final, expected 1"
        )
    log(f"  pass: bethesda_final has exactly 1 authoritative feeder")

    n_unresolved = con.execute(f"""
        SELECT COUNT(*) FROM {CPM}
        WHERE bethesda_derivation_methods ILIKE '%unresolved%'
    """).fetchone()[0]
    if n_unresolved != 0:
        raise SystemExit(
            f"FAIL: {n_unresolved} patients have 'unresolved' in derivation_methods"
        )
    log("  pass: 0 patients with 'unresolved'")


def main() -> int:
    log = TeeLogger(RUN_LOG)
    t0 = time.time()
    summaries: dict = {"script": SCRIPT_TAG, "run_date": RUN_DATE}
    try:
        log("=" * 78)
        log(f"=== START {SCRIPT_TAG}")
        log(f"started_at: {utc_now()}")

        con = connect_locked()
        log(f"connected to {PUBLICATION_DB}")

        schema_verify(con, log)
        summaries["convention"] = lock_convention(con, log)
        summaries["preop_rollup"] = materialize_preop_rollup(con, log)
        summaries["index_nodule"] = materialize_index_nodule(con, log)
        summaries["prior_distribution"] = log_prior_bethesda_distribution(con, log)
        n_rows, n_cols = rebuild_cpm(con, log)
        summaries["cpm_after_rebuild"] = {"rows": n_rows, "cols": n_cols}
        summaries["registry_pin_actions"] = update_registry_pins(con, log)
        write_distribution_report(con, log, summaries["prior_distribution"],
                                  summaries["preop_rollup"],
                                  summaries["index_nodule"])
        drop_staging(con, log)
        final_assertions(con, log)

        elapsed = time.time() - t0
        summaries["elapsed_seconds"] = round(elapsed, 1)
        DECISION_LOG.write_text(json.dumps(summaries, indent=2, default=str))
        log(f"\nwrote {DECISION_LOG}")
        log(f"=== END {SCRIPT_TAG} elapsed={elapsed:.1f}s")
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
