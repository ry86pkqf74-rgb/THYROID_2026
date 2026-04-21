#!/usr/bin/env python3
"""
Script 299 — Build canonical_us_nodule_master_v1 (per-nodule integrated master)

Date:    2026-04-21

Architecture
============
Integrates five sources at the per-nodule grain into a single master table.
Base identity comes from canonical_us_nodule_characteristics_v1 (37,016 rows /
6,126 pts) built by Script 246 from imaging_nodule_master_v1 + tirads_llm overlay.

LEFT-joined enrichment sources (COALESCE — base always wins):
  1. tirads_v2_nodules_raw         (11,914 rows / 3,021 pts) — Qwen2.5-32B
  2. tirads_llm_extracted_v2       (5,636 rows / 1,429 pts, if present)
  3. Parsed LLM staging:
       a. tirads_granular_parsed_v1   from note_entities_llm_tirads_granular
       b. us_nodule_dynamics_parsed_v1 from note_entities_llm_us_nodule_dynamics
  4. imaging_fna_linkage_v3        (9,911 rows) — FNA-to-nodule linkage flag

Join strategy: base key is (research_id, exam_date, laterality,
nodule_index_within_exam). For enrichment tables without per-nodule matching
keys, deduplicate to 1 row/research_id via ROW_NUMBER and join on
research_id only.  COALESCE(base, v2, llm) — never regress a non-NULL base
value.

Tables WRITTEN
--------------
  main.canonical_us_nodule_master_v1                  (NEW master)
  manuscript_workspace.tirads_granular_parsed_v1      (staging)
  manuscript_workspace.us_nodule_dynamics_parsed_v1   (staging)
  manuscript_workspace.tirads_v1_v2_discordance_v1    (audit)

Tables READ
-----------
  main.canonical_us_nodule_characteristics_v1   (base)
  main.tirads_v2_nodules_raw
  main.tirads_llm_extracted_v2                  (if present)
  main.note_entities_llm_tirads_granular
  main.note_entities_llm_us_nodule_dynamics
  main.imaging_fna_linkage_v3
  main.imaging_nodule_master_v1                 (redundancy check)
  main.canonical_patient_master                 (CPM invariants)

Usage:
    python 299_canonical_us_nodule_master_v1.py            # dry-run
    python 299_canonical_us_nodule_master_v1.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

# ── constants ────────────────────────────────────────────────────────────────

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DECISION_LOG_PATH = OUTPUT_DIR / "299_decision_log.json"

SCRIPT_TAG = "Script 299"
RUN_DATE = datetime.datetime.utcnow().strftime("%Y-%m-%d")

BASE_TABLE = "canonical_us_nodule_characteristics_v1"
V2_NOD_TABLE = "tirads_v2_nodules_raw"
LEGACY_LLM_TABLE = "tirads_llm_extracted_v2"
FNA_LINKAGE_TABLE = "imaging_fna_linkage_v3"
MASTER_TABLE = "canonical_us_nodule_master_v1"
DISCORDANCE_TABLE = "tirads_v1_v2_discordance_v1"

TIRADS_GRANULAR_SRC = "note_entities_llm_tirads_granular"
DYNAMICS_SRC = "note_entities_llm_us_nodule_dynamics"
TIRADS_GRANULAR_PARSED = "tirads_granular_parsed_v1"
DYNAMICS_PARSED = "us_nodule_dynamics_parsed_v1"

EXPECTED_BASE_ROWS = 37_016
EXPECTED_BASE_PTS = 6_126
EXPECTED_V2_ROWS = 11_914
EXPECTED_V2_PTS = 3_021
EXPECTED_CPM_ROWS = 10_871


# ── helpers ──────────────────────────────────────────────────────────────────

def ts_utc() -> str:
    now = datetime.datetime.utcnow()
    return now.strftime("%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


def log(msg: str) -> None:
    print(f"[{ts_utc()}] {msg}", flush=True)


def cpm_invariants(con, label: str = "") -> None:
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
        FROM main.canonical_patient_master
    """).fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != EXPECTED_CPM_ROWS or r[1] != EXPECTED_CPM_ROWS or r[2] != 0:
        raise SystemExit("CPM invariant violation")


def table_exists(con, table_name: str, schema: str = "main") -> bool:
    return con.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [PUBLICATION_DB, schema, table_name],
    ).fetchone()[0] > 0


def get_columns(con, table_name: str, schema: str = "main") -> set[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [PUBLICATION_DB, schema, table_name],
    ).fetchall()
    return {r[0] for r in rows}


def table_stats(con, fq_table: str) -> tuple[int, int]:
    r = con.execute(
        f"SELECT COUNT(*), COUNT(DISTINCT research_id) FROM {fq_table}"
    ).fetchone()
    return int(r[0]), int(r[1])


# ── Step 1: Parse LLM JSON into staging tables ──────────────────────────────

def parse_llm_staging(con, *, commit: bool) -> dict:
    """Parse entity arrays from LLM result_json into flat staging tables."""
    out: dict = {}

    for src_table, dest_table, parse_sql, empty_sql, label in [
        (
            TIRADS_GRANULAR_SRC,
            TIRADS_GRANULAR_PARSED,
            f"""
            CREATE OR REPLACE TABLE manuscript_workspace.{TIRADS_GRANULAR_PARSED} AS
            WITH src AS (
                SELECT research_id, note_row_id,
                       json_extract(CAST(result_json AS JSON), '$.entities') AS entities_arr
                FROM {TIRADS_GRANULAR_SRC}
                WHERE result_json IS NOT NULL
                  AND json_type(json_extract(CAST(result_json AS JSON), '$.entities')) = 'ARRAY'
            ),
            flat AS (
                SELECT research_id, note_row_id,
                       UNNEST(CAST(entities_arr AS JSON[])) AS entity
                FROM src
            )
            SELECT
                CAST(research_id AS BIGINT)                                    AS research_id,
                note_row_id,
                json_extract_string(entity, '$.laterality')                    AS laterality,
                TRY_CAST(json_extract_string(entity, '$.nodule_index') AS INTEGER) AS nodule_index,
                json_extract_string(entity, '$.composition')                   AS composition,
                json_extract_string(entity, '$.echogenicity')                  AS echogenicity,
                json_extract_string(entity, '$.shape')                         AS shape,
                json_extract_string(entity, '$.margin')                        AS margin,
                json_extract_string(entity, '$.echogenic_foci')                AS echogenic_foci,
                TRY_CAST(json_extract_string(entity, '$.size_cm') AS DOUBLE)   AS size_cm,
                TRY_CAST(json_extract_string(entity, '$.tirads_points') AS INTEGER) AS tirads_points,
                json_extract_string(entity, '$.tirads_category')               AS tirads_category,
                json_extract_string(entity, '$.evidence_text')                 AS evidence_text
            FROM flat
            """,
            f"""
            CREATE OR REPLACE TABLE manuscript_workspace.{TIRADS_GRANULAR_PARSED} AS
            SELECT NULL::BIGINT AS research_id, NULL::BIGINT AS note_row_id,
                   NULL::VARCHAR AS laterality, NULL::INTEGER AS nodule_index,
                   NULL::VARCHAR AS composition, NULL::VARCHAR AS echogenicity,
                   NULL::VARCHAR AS shape, NULL::VARCHAR AS margin,
                   NULL::VARCHAR AS echogenic_foci, NULL::DOUBLE AS size_cm,
                   NULL::INTEGER AS tirads_points, NULL::VARCHAR AS tirads_category,
                   NULL::VARCHAR AS evidence_text
            WHERE FALSE
            """,
            "tirads_granular",
        ),
        (
            DYNAMICS_SRC,
            DYNAMICS_PARSED,
            f"""
            CREATE OR REPLACE TABLE manuscript_workspace.{DYNAMICS_PARSED} AS
            WITH src AS (
                SELECT research_id, note_row_id,
                       json_extract(CAST(result_json AS JSON), '$.entities') AS entities_arr
                FROM {DYNAMICS_SRC}
                WHERE result_json IS NOT NULL
                  AND json_type(json_extract(CAST(result_json AS JSON), '$.entities')) = 'ARRAY'
            ),
            flat AS (
                SELECT research_id, note_row_id,
                       UNNEST(CAST(entities_arr AS JSON[])) AS entity
                FROM src
            )
            SELECT
                CAST(research_id AS BIGINT)                                         AS research_id,
                note_row_id,
                json_extract_string(entity, '$.laterality')                         AS laterality,
                TRY_CAST(json_extract_string(entity, '$.nodule_index') AS INTEGER)  AS nodule_index,
                TRY_CAST(json_extract_string(entity, '$.prior_size_mm') AS DOUBLE)  AS prior_size_mm,
                TRY_CAST(json_extract_string(entity, '$.current_size_mm') AS DOUBLE) AS current_size_mm,
                TRY_CAST(json_extract_string(entity, '$.interval_growth_mm') AS DOUBLE) AS interval_growth_mm,
                json_extract_string(entity, '$.dynamics_category')                  AS dynamics_category,
                json_extract_string(entity, '$.evidence_text')                      AS evidence_text
            FROM flat
            """,
            f"""
            CREATE OR REPLACE TABLE manuscript_workspace.{DYNAMICS_PARSED} AS
            SELECT NULL::BIGINT AS research_id, NULL::BIGINT AS note_row_id,
                   NULL::VARCHAR AS laterality, NULL::INTEGER AS nodule_index,
                   NULL::DOUBLE AS prior_size_mm, NULL::DOUBLE AS current_size_mm,
                   NULL::DOUBLE AS interval_growth_mm, NULL::VARCHAR AS dynamics_category,
                   NULL::VARCHAR AS evidence_text
            WHERE FALSE
            """,
            "us_dynamics",
        ),
    ]:
        has_src = table_exists(con, src_table)
        if has_src:
            src_rows = con.execute(f"SELECT COUNT(*) FROM {src_table}").fetchone()[0]
            src_with_arr = con.execute(f"""
                SELECT COUNT(*) FROM {src_table}
                WHERE result_json IS NOT NULL
                  AND json_type(json_extract(CAST(result_json AS JSON), '$.entities')) = 'ARRAY'
            """).fetchone()[0]
            log(f"  {label}: source {src_table} has {src_rows} rows, "
                f"{src_with_arr} with valid entities array")
        else:
            log(f"  {label}: source {src_table} NOT FOUND — creating empty staging table")

        if commit:
            if has_src:
                t1 = time.time()
                con.execute(parse_sql)
                elapsed = time.time() - t1
            else:
                con.execute(empty_sql)
                elapsed = 0.0

            rows, pts = table_stats(
                con, f"manuscript_workspace.{dest_table}"
            )
            log(f"  {label}: parsed -> manuscript_workspace.{dest_table} "
                f"({rows} rows, {pts} patients, {elapsed:.1f}s)")
            out[label] = {"rows": rows, "patients": pts, "source_present": has_src}
        else:
            out[label] = {"source_present": has_src, "dry_run": True}
            if has_src:
                log(f"  {label}: would parse {src_with_arr} rows "
                    f"-> manuscript_workspace.{dest_table}")

    return out


# ── Step 2 + 3: Build master table with nodule_master_id ────────────────────

def build_master_sql(con) -> str:
    """Dynamically build the master CREATE SQL based on available columns."""
    has_v2 = table_exists(con, V2_NOD_TABLE)
    v2_cols = get_columns(con, V2_NOD_TABLE) if has_v2 else set()
    has_legacy = table_exists(con, LEGACY_LLM_TABLE)
    has_fna = table_exists(con, FNA_LINKAGE_TABLE)

    def v2c(col: str, fallback: str = "VARCHAR") -> str:
        """Reference a v2 column if it exists, else NULL with proper type."""
        if col in v2_cols:
            return f"v2.{col}"
        return f"NULL::{fallback}"

    # ── v2 CTE ──────────────────────────────────────────────────────────
    if has_v2:
        v2_cte = f"""
v2_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY tirads_total_points DESC NULLS LAST,
                     size_cm_max DESC NULLS LAST
        ) AS _rn
    FROM {V2_NOD_TABLE}
),
v2 AS (SELECT * FROM v2_ranked WHERE _rn = 1),"""
        v2_join = "LEFT JOIN v2 ON TRY_CAST(v2.research_id AS INTEGER) = b.research_id"
        v2_source = "v2.research_id IS NOT NULL"
    else:
        v2_cte = """
v2 AS (SELECT NULL::INTEGER AS research_id WHERE FALSE),"""
        v2_join = "LEFT JOIN v2 ON v2.research_id = b.research_id"
        v2_source = "FALSE"

    # ── legacy LLM CTE ──────────────────────────────────────────────────
    if has_legacy:
        legacy_cte = f"""
llm_leg_ranked AS (
    SELECT
        CAST(research_id AS INTEGER) AS research_id,
        composition_pts, echogenicity_pts, shape_pts, margin_pts, foci_pts,
        total_pts_2017, tirads_level_2017,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(research_id AS INTEGER)
            ORDER BY n_categories_scored DESC NULLS LAST,
                     total_pts_2017 DESC NULLS LAST
        ) AS _rn
    FROM {LEGACY_LLM_TABLE}
),
llm_leg AS (SELECT * FROM llm_leg_ranked WHERE _rn = 1),"""
        legacy_join = "LEFT JOIN llm_leg leg ON leg.research_id = b.research_id"
        leg_source = "leg.research_id IS NOT NULL"
    else:
        legacy_cte = ""
        legacy_join = ""
        leg_source = "FALSE"

    # ── FNA CTE ─────────────────────────────────────────────────────────
    if has_fna:
        fna_cte = f"""
fna_flag AS (
    SELECT DISTINCT CAST(research_id AS INTEGER) AS research_id
    FROM {FNA_LINKAGE_TABLE}
),"""
        fna_join = "LEFT JOIN fna_flag fna ON fna.research_id = b.research_id"
        fna_source = "fna.research_id IS NOT NULL"
    else:
        fna_cte = ""
        fna_join = ""
        fna_source = "FALSE"

    sql = f"""
CREATE OR REPLACE TABLE main.{MASTER_TABLE} AS
WITH
{v2_cte}
{legacy_cte}
-- Deduplicate tirads granular parsed to 1 row per research_id
tgp_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY tirads_points DESC NULLS LAST, size_cm DESC NULLS LAST
        ) AS _rn
    FROM manuscript_workspace.{TIRADS_GRANULAR_PARSED}
),
tgp AS (SELECT * FROM tgp_ranked WHERE _rn = 1),

-- Deduplicate dynamics parsed to 1 row per research_id
dyn_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY research_id
            ORDER BY interval_growth_mm DESC NULLS LAST,
                     prior_size_mm DESC NULLS LAST
        ) AS _rn
    FROM manuscript_workspace.{DYNAMICS_PARSED}
),
dyn AS (SELECT * FROM dyn_ranked WHERE _rn = 1),

{fna_cte}

-- ── master assembly ────────────────────────────────────────────────
assembled AS (
    SELECT
        b.research_id,
        b.exam_date,
        b.laterality,
        b.nodule_index_within_exam,

        -- size (base wins)
        COALESCE(b.size_cm_max, {v2c('size_cm_max', 'DOUBLE')}, tgp.size_cm)
            AS size_cm,

        -- TI-RADS features (base wins, then v2, then LLM granular)
        COALESCE(b.composition, {v2c('composition')}, tgp.composition)
            AS composition,
        COALESCE(b.echogenicity, {v2c('echogenicity')}, tgp.echogenicity)
            AS echogenicity,
        COALESCE(b.shape, {v2c('shape')}, tgp.shape)
            AS shape,
        COALESCE(b.margins, {v2c('margin')}, tgp.margin)
            AS margin,
        COALESCE(b.echogenic_foci, CAST({v2c('echogenic_foci')} AS VARCHAR), tgp.echogenic_foci)
            AS echogenic_foci,

        -- TI-RADS scores (from base/original sources, not enriched)
        b.tirads_score_2017,
        b.tirads_category_v2,
        b.tirads_level_2017,

        -- Points total (enriched)
        COALESCE(
            b.tirads_score_2017,
            {v2c('tirads_total_points', 'INTEGER')},
            tgp.tirads_points
        ) AS tirads_points_total,

        -- Per-component points (base wins via Script 246 overlay, then v2)
        COALESCE(b.composition_pts,  {v2c('composition_points',  'INTEGER')}) AS tirads_points_composition,
        COALESCE(b.echogenicity_pts, {v2c('echogenicity_points', 'INTEGER')}) AS tirads_points_echogenicity,
        COALESCE(b.shape_pts,        {v2c('shape_points',        'INTEGER')}) AS tirads_points_shape,
        COALESCE(b.margin_pts,       {v2c('margin_points',       'INTEGER')}) AS tirads_points_margin,
        COALESCE(b.foci_pts,         {v2c('foci_points',         'INTEGER')}) AS tirads_points_foci,

        -- v2-exclusive fields
        {v2c('chammas_type')}                                    AS chammas_type,
        {v2c('elastography_category')}                           AS elastography_category,
        {v2c('extrathyroidal_extension_on_us')}                  AS extrathyroidal_extension_on_us,
        {v2c('fna_recommended_this_nodule', 'BOOLEAN')}          AS fna_recommended_this_nodule,

        -- interval growth (v2 primary, dynamics LLM fallback)
        COALESCE(
            {v2c('interval_growth_flag', 'BOOLEAN')},
            CASE WHEN dyn.interval_growth_mm > 0 THEN TRUE
                 WHEN dyn.interval_growth_mm IS NOT NULL THEN FALSE
            END
        ) AS interval_growth_flag,

        -- provenance flags
        TRUE                                                       AS source_base,
        CASE WHEN {v2_source}  THEN TRUE ELSE FALSE END            AS source_tirads_v2,
        CASE WHEN b.source_tables LIKE '%tirads_llm%'
                  OR tgp.research_id IS NOT NULL
                  OR {leg_source}
             THEN TRUE ELSE FALSE END                              AS source_tirads_llm,
        CASE WHEN dyn.research_id IS NOT NULL
             THEN TRUE ELSE FALSE END                              AS source_dynamics_llm,
        CASE WHEN {fna_source}
             THEN TRUE ELSE FALSE END                              AS source_fna_linkage

    FROM {BASE_TABLE} b
    {v2_join}
    {legacy_join}
    LEFT JOIN tgp ON tgp.research_id = b.research_id
    LEFT JOIN dyn ON dyn.research_id = b.research_id
    {fna_join}
)

-- Step 3: add surrogate nodule_master_id
SELECT
    a.*,
    ROW_NUMBER() OVER (
        ORDER BY a.research_id, a.exam_date, a.laterality, a.nodule_index_within_exam
    ) AS nodule_master_id
FROM assembled a
"""
    return sql


# ── Step 4: Discordance queue ────────────────────────────────────────────────

def build_discordance_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE manuscript_workspace.{DISCORDANCE_TABLE} AS
WITH
tr_rank AS (
    SELECT unnest(['TR1','TR2','TR3','TR4','TR5']) AS cat,
           unnest([1, 2, 3, 4, 5])                 AS rank_val
),
-- patient-level worst from base
base_worst AS (
    SELECT
        b.research_id,
        MAX(r.rank_val) AS base_max_rank,
        MAX(b.tirads_category_v2) FILTER (WHERE r.rank_val = (
            SELECT MAX(r2.rank_val) FROM {BASE_TABLE} b2
            JOIN tr_rank r2 ON r2.cat = b2.tirads_category_v2
            WHERE b2.research_id = b.research_id
        )) AS base_worst_category
    FROM {BASE_TABLE} b
    JOIN tr_rank r ON r.cat = b.tirads_category_v2
    GROUP BY b.research_id
),
-- patient-level worst from v2
v2_worst AS (
    SELECT
        CAST(v.research_id AS INTEGER) AS research_id,
        MAX(r.rank_val) AS v2_max_rank,
        MAX(v.tirads_category) FILTER (WHERE r.rank_val = (
            SELECT MAX(r2.rank_val)
            FROM {V2_NOD_TABLE} v2
            JOIN tr_rank r2 ON r2.cat = v2.tirads_category
            WHERE v2.research_id = v.research_id
        )) AS v2_worst_category
    FROM {V2_NOD_TABLE} v
    JOIN tr_rank r ON r.cat = v.tirads_category
    GROUP BY 1
)
SELECT
    b.research_id,
    b.base_worst_category,
    b.base_max_rank,
    v.v2_worst_category,
    v.v2_max_rank,
    ABS(b.base_max_rank - v.v2_max_rank) AS abs_rank_diff,
    CASE WHEN b.base_max_rank > v.v2_max_rank THEN 'base_higher'
         WHEN b.base_max_rank < v.v2_max_rank THEN 'v2_higher'
         ELSE 'concordant'
    END AS direction,
    CASE WHEN ABS(b.base_max_rank - v.v2_max_rank) >= 2 THEN 'HIGH'
         ELSE 'MEDIUM'
    END AS review_priority
FROM base_worst b
JOIN v2_worst v USING (research_id)
WHERE b.base_worst_category <> v.v2_worst_category
"""


# ── Step 5: Redundancy check ────────────────────────────────────────────────

def check_redundancy(con) -> dict:
    """Compare imaging_nodule_master_v1 vs the base table."""
    out: dict = {}

    has_inm = table_exists(con, "imaging_nodule_master_v1")
    if not has_inm:
        log("  imaging_nodule_master_v1 not found — redundancy check skipped")
        out["skipped"] = True
        return out

    inm_cols = get_columns(con, "imaging_nodule_master_v1")
    base_cols = get_columns(con, BASE_TABLE)

    inm_rows, inm_pts = table_stats(con, "imaging_nodule_master_v1")
    base_rows, base_pts = table_stats(con, BASE_TABLE)

    shared = inm_cols & base_cols
    inm_only = sorted(inm_cols - base_cols)
    base_only = sorted(base_cols - inm_cols)

    log(f"  imaging_nodule_master_v1: {inm_rows:,} rows, {inm_pts:,} pts, "
        f"{len(inm_cols)} cols")
    log(f"  {BASE_TABLE}: {base_rows:,} rows, {base_pts:,} pts, "
        f"{len(base_cols)} cols")
    log(f"  shared cols: {len(shared)}")
    log(f"  inm_v1-only cols ({len(inm_only)}): {inm_only[:15]}"
        + (" ..." if len(inm_only) > 15 else ""))
    log(f"  base-only cols ({len(base_only)}): {base_only[:15]}"
        + (" ..." if len(base_only) > 15 else ""))

    same_grain = inm_rows == base_rows and inm_pts == base_pts
    log(f"  same grain: {same_grain}")

    if same_grain and len(inm_only) == 0:
        recommendation = (
            "imaging_nodule_master_v1 is semantically redundant with "
            f"{BASE_TABLE}. Recommend deprecation in v1_1."
        )
    elif same_grain:
        recommendation = (
            f"Same grain but imaging_nodule_master_v1 has {len(inm_only)} "
            "unique columns. Review for integration or deprecation."
        )
    else:
        recommendation = (
            "Different grain or row counts — tables are not directly redundant. "
            f"inm_v1={inm_rows} vs base={base_rows}."
        )
    log(f"  RECOMMENDATION: {recommendation}")

    out.update({
        "inm_rows": inm_rows, "inm_pts": inm_pts,
        "base_rows": base_rows, "base_pts": base_pts,
        "shared_cols": len(shared),
        "inm_only_cols": inm_only,
        "base_only_cols": base_only,
        "same_grain": same_grain,
        "recommendation": recommendation,
    })
    return out


# ── Invariant verification ──────────────────────────────────────────────────

def verify_invariants(con) -> dict:
    """Post-build invariant checks on the master table."""
    out: dict = {}

    master_rows, master_pts = table_stats(con, f"main.{MASTER_TABLE}")
    log(f"  {MASTER_TABLE}: {master_rows:,} rows, {master_pts:,} patients")

    # Grain uniqueness
    n_unique = con.execute(f"""
        SELECT COUNT(DISTINCT (research_id, exam_date, laterality,
                               nodule_index_within_exam))
        FROM main.{MASTER_TABLE}
    """).fetchone()[0]
    grain_ok = master_rows == n_unique
    log(f"  grain unique: {grain_ok} "
        f"(rows={master_rows:,}, distinct_key={n_unique:,})")

    # Patient count ±5
    pts_ok = abs(master_pts - EXPECTED_BASE_PTS) <= 5
    log(f"  patient count: {master_pts:,} "
        f"(expected {EXPECTED_BASE_PTS} ± 5, ok={pts_ok})")

    # TIRADS non-null fill >= base (never regress)
    base_fill = con.execute(f"""
        SELECT COUNT(composition), COUNT(echogenicity), COUNT(shape),
               COUNT(margins), COUNT(echogenic_foci)
        FROM {BASE_TABLE}
    """).fetchone()
    master_fill = con.execute(f"""
        SELECT COUNT(composition), COUNT(echogenicity), COUNT(shape),
               COUNT(margin), COUNT(echogenic_foci)
        FROM main.{MASTER_TABLE}
    """).fetchone()
    fill_ok = all(int(m) >= int(b) for m, b in zip(master_fill, base_fill))
    labels = ["composition", "echogenicity", "shape", "margin", "foci"]
    log(f"  TIRADS fill >= base: {fill_ok}")
    for lbl, bv, mv in zip(labels, base_fill, master_fill):
        flag = "OK" if int(mv) >= int(bv) else "REGRESSED"
        log(f"    {lbl:15s}  base={int(bv):>6,}  master={int(mv):>6,}  [{flag}]")

    # Source coverage
    cov = con.execute(f"""
        SELECT
            COUNT(*) FILTER (WHERE source_tirads_v2)   AS n_v2,
            COUNT(*) FILTER (WHERE source_tirads_llm)  AS n_llm,
            COUNT(*) FILTER (WHERE source_dynamics_llm) AS n_dyn,
            COUNT(*) FILTER (WHERE source_fna_linkage) AS n_fna
        FROM main.{MASTER_TABLE}
    """).fetchone()
    log(f"  source coverage: v2={cov[0]:,} llm={cov[1]:,} "
        f"dyn={cov[2]:,} fna={cov[3]:,}")

    checks_ok = grain_ok and pts_ok and fill_ok
    if not checks_ok:
        log("  ⚠ INVARIANT VIOLATIONS DETECTED — review required")

    out.update({
        "rows": master_rows, "patients": master_pts,
        "grain_unique": grain_ok, "pts_ok": pts_ok, "fill_ok": fill_ok,
        "source_v2": int(cov[0]), "source_llm": int(cov[1]),
        "source_dyn": int(cov[2]), "source_fna": int(cov[3]),
        "all_ok": checks_ok,
    })
    return out


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Script 299 — Build canonical_us_nodule_master_v1"
    )
    ap.add_argument("--commit", action="store_true",
                    help="Apply changes (default: dry-run)")
    args = ap.parse_args()
    commit = args.commit

    t0 = time.time()
    run_ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    decisions: dict = {
        "script": SCRIPT_TAG,
        "run_ts": run_ts,
        "run_date": RUN_DATE,
        "commit": commit,
        "steps": {},
    }

    log("=" * 72)
    log(f"=== START {Path(__file__).name} "
        f"{'(COMMIT)' if commit else '(DRY-RUN)'}")
    log("=" * 72)

    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    cpm_invariants(con, "pre")

    # ── Step 0: Baseline stats ──────────────────────────────────────────
    log("")
    log("STEP 0 — Baseline stats")

    base_rows, base_pts = table_stats(con, BASE_TABLE)
    log(f"  {BASE_TABLE}: {base_rows:,} rows, {base_pts:,} patients")

    has_v2 = table_exists(con, V2_NOD_TABLE)
    if has_v2:
        v2_rows, v2_pts = table_stats(con, V2_NOD_TABLE)
        v2_cols = get_columns(con, V2_NOD_TABLE)
        log(f"  {V2_NOD_TABLE}: {v2_rows:,} rows, {v2_pts:,} patients, "
            f"{len(v2_cols)} cols")
    else:
        v2_rows, v2_pts = 0, 0
        log(f"  {V2_NOD_TABLE}: NOT FOUND")

    has_legacy = table_exists(con, LEGACY_LLM_TABLE)
    if has_legacy:
        leg_rows, leg_pts = table_stats(con, LEGACY_LLM_TABLE)
        log(f"  {LEGACY_LLM_TABLE}: {leg_rows:,} rows, {leg_pts:,} patients")
    else:
        log(f"  {LEGACY_LLM_TABLE}: NOT FOUND (may have been archived by Script 221)")

    has_fna = table_exists(con, FNA_LINKAGE_TABLE)
    if has_fna:
        fna_rows, fna_pts = table_stats(con, FNA_LINKAGE_TABLE)
        log(f"  {FNA_LINKAGE_TABLE}: {fna_rows:,} rows, {fna_pts:,} patients")
    else:
        log(f"  {FNA_LINKAGE_TABLE}: NOT FOUND")

    has_tg_src = table_exists(con, TIRADS_GRANULAR_SRC)
    has_dyn_src = table_exists(con, DYNAMICS_SRC)
    log(f"  LLM sources: {TIRADS_GRANULAR_SRC}={'present' if has_tg_src else 'ABSENT'}  "
        f"{DYNAMICS_SRC}={'present' if has_dyn_src else 'ABSENT'}")

    decisions["steps"]["0_baseline"] = {
        "base": {"rows": base_rows, "patients": base_pts},
        "v2": {"rows": v2_rows, "patients": v2_pts, "present": has_v2},
        "legacy_llm": {"present": has_legacy},
        "fna_linkage": {"present": has_fna},
        "llm_tirads_granular": {"present": has_tg_src},
        "llm_dynamics": {"present": has_dyn_src},
    }

    # ── Step 1: Parse LLM JSON into staging tables ──────────────────────
    log("")
    log("STEP 1 — Parse LLM JSON into staging tables")
    step1 = parse_llm_staging(con, commit=commit)
    decisions["steps"]["1_llm_parse"] = step1

    # ── Step 2 + 3: Build master table with nodule_master_id ────────────
    log("")
    log("STEP 2+3 — Build master table with nodule_master_id")

    master_sql = build_master_sql(con)

    if commit:
        t1 = time.time()
        con.execute(master_sql)
        elapsed = time.time() - t1
        log(f"  built main.{MASTER_TABLE} in {elapsed:.1f}s")

        m_rows, m_pts = table_stats(con, f"main.{MASTER_TABLE}")
        log(f"  rows={m_rows:,}  patients={m_pts:,}")

        n_master_id = con.execute(f"""
            SELECT MAX(nodule_master_id) FROM main.{MASTER_TABLE}
        """).fetchone()[0]
        log(f"  nodule_master_id range: 1 .. {n_master_id:,}")

        decisions["steps"]["2_3_master_build"] = {
            "rows": m_rows, "patients": m_pts,
            "max_nodule_master_id": int(n_master_id or 0),
            "elapsed_s": round(elapsed, 1),
        }
    else:
        log("  (dry-run — SQL built but not executed)")
        log(f"  SQL length: {len(master_sql):,} chars")
        log(f"  expected rows: ~{base_rows:,} (same grain as base)")
        decisions["steps"]["2_3_master_build"] = {
            "dry_run": True, "sql_len": len(master_sql),
        }

    # ── Step 4: Discordance queue ───────────────────────────────────────
    log("")
    log("STEP 4 — Discordance queue")

    if not has_v2:
        log("  v2 table absent — discordance check skipped")
        decisions["steps"]["4_discordance"] = {"skipped": "no_v2_table"}
    elif commit:
        disc_sql = build_discordance_sql()
        t1 = time.time()
        con.execute(disc_sql)
        elapsed = time.time() - t1

        d_rows = con.execute(
            f"SELECT COUNT(*) FROM manuscript_workspace.{DISCORDANCE_TABLE}"
        ).fetchone()[0]
        dir_dist = con.execute(f"""
            SELECT direction, COUNT(*) AS n
            FROM manuscript_workspace.{DISCORDANCE_TABLE}
            GROUP BY direction ORDER BY direction
        """).fetchall()

        log(f"  manuscript_workspace.{DISCORDANCE_TABLE}: {d_rows:,} rows "
            f"({elapsed:.1f}s)")
        for d, n in dir_dist:
            log(f"    {d}: {n:,}")

        decisions["steps"]["4_discordance"] = {
            "rows": d_rows,
            "direction_distribution": {d: int(n) for d, n in dir_dist},
            "elapsed_s": round(elapsed, 1),
        }
    else:
        log("  (dry-run — discordance table not written)")
        decisions["steps"]["4_discordance"] = {"dry_run": True}

    # ── Step 5: Redundancy check ────────────────────────────────────────
    log("")
    log("STEP 5 — Redundancy check (imaging_nodule_master_v1 vs base)")
    step5 = check_redundancy(con)
    decisions["steps"]["5_redundancy"] = step5

    # ── Step 6: Invariant verification ──────────────────────────────────
    log("")
    if commit:
        log("STEP 6 — Invariant verification")
        step6 = verify_invariants(con)
        decisions["steps"]["6_invariants"] = step6
    else:
        log("STEP 6 — Invariant verification (skipped in dry-run)")
        decisions["steps"]["6_invariants"] = {"dry_run": True}

    # ── CPM post-check ──────────────────────────────────────────────────
    log("")
    cpm_invariants(con, "post")

    # ── Write decision log ──────────────────────────────────────────────
    DECISION_LOG_PATH.write_text(json.dumps(decisions, indent=2, default=str))
    log(f"  decision log -> {DECISION_LOG_PATH.name}")

    elapsed = time.time() - t0
    log("")
    log("=" * 72)
    log(f"=== END {Path(__file__).name}  elapsed={elapsed:.1f}s")
    if not commit:
        log("    (dry-run — re-run with --commit to apply)")
    log("=" * 72)


if __name__ == "__main__":
    main()
