#!/usr/bin/env python3
"""
THYROID_2026 — Script 212: NLP Entity Patient-Level Rollup for Canonical
Database: thyroid_ete_fix_20260413

For all NLP domains (23 LLM + 7 non-LLM), creates patient-level rollups
and adds them to canonical_patient_master_v1 as nlp_-prefixed columns.
These are ADDITIVE — they never override structured data columns.

Cross-validation concordance tiers:
  Tier 1 (>80%): pathology (90.6%), TIRADS (89.0%), cervical_ln (91.0%), tg_kinetics (93.5%)
  Tier 2 (<80%): recurrence (69.0%), vascular_invasion (53.9%)
  Tier 3: 17 remaining original domains (minimal rollup)

Run:
  .venv/bin/python scripts/212_nlp_entity_rollup.py [--dry-run]
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from motherduck_client import get_token  # noqa: E402

DB = "thyroid_ete_fix_20260413"
CANONICAL = "canonical_patient_master_v1"

# -----------------------------------------------------------------------
# LLM entity parsing CTE template
# -----------------------------------------------------------------------
LLM_ENTITY_PARSE_CTE = """
{alias}_parsed AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        note_row_id,
        json_extract(CAST(result_json AS JSON), '$.entities') AS entities_arr
    FROM {table}
    WHERE result_json IS NOT NULL
      AND CAST(result_json AS VARCHAR) NOT LIKE '%"entities": []%'
      AND json_type(json_extract(CAST(result_json AS JSON), '$.entities')) = 'ARRAY'
),
{alias}_flat AS (
    SELECT
        research_id,
        note_row_id,
        UNNEST(CAST(entities_arr AS JSON[])) AS entity
    FROM {alias}_parsed
),
{alias}_ext AS (
    SELECT
        research_id,
        note_row_id,
        json_extract_string(entity, '$.entity_type') AS entity_type,
        json_extract_string(entity, '$.entity_value') AS entity_value,
        json_extract_string(entity, '$.entity_date') AS entity_date,
        COALESCE(TRY_CAST(json_extract(entity, '$.confidence') AS DOUBLE), 0) AS confidence,
        json_extract_string(entity, '$.present_or_negated') AS present_or_negated
    FROM {alias}_flat
),
{alias}_pos AS (
    SELECT * FROM {alias}_ext
    WHERE confidence >= 0.5
      AND (present_or_negated = 'present' OR present_or_negated IS NULL)
)
"""

# -----------------------------------------------------------------------
# Tier 1 rollup SQL generators (detailed columns)
# -----------------------------------------------------------------------

def tier1_pathology_sql() -> str:
    alias = "path"
    table = "note_entities_llm_pathology"
    parse = LLM_ENTITY_PARSE_CTE.format(alias=alias, table=table)
    return f"""
{parse},
nlp_pathology AS (
    SELECT
        research_id,
        TRUE AS nlp_path_has_data,
        COUNT(*) AS nlp_path_n_entities,
        COUNT(DISTINCT note_row_id) AS nlp_path_n_notes,
        MODE(entity_value) FILTER (WHERE entity_type ILIKE '%histolog%' OR entity_type ILIKE '%surgical_path%') AS nlp_path_histology_mentioned,
        BOOL_OR(entity_type ILIKE '%ete%' OR entity_type ILIKE '%extrathyroid%' OR entity_value ILIKE '%extrathyroid%') AS nlp_path_ete_mentioned,
        BOOL_OR(entity_type ILIKE '%vascular%' OR entity_type ILIKE '%angioinvasion%' OR entity_value ILIKE '%vascular invasion%') AS nlp_path_vasc_inv_mentioned,
        BOOL_OR(entity_type ILIKE '%lymph_node%' OR entity_value ILIKE '%lymph node positive%' OR entity_value ILIKE '%metasta%') AS nlp_path_ln_positive_mentioned,
        BOOL_OR(entity_type ILIKE '%margin%' OR entity_value ILIKE '%margin%') AS nlp_path_margin_mentioned,
        BOOL_OR(entity_type ILIKE '%multifocal%' OR entity_value ILIKE '%multifocal%') AS nlp_path_multifocal_mentioned
    FROM {alias}_pos
    GROUP BY research_id
)"""


def tier1_tirads_sql() -> str:
    alias = "tir"
    table = "note_entities_llm_tirads_granular"
    parse = LLM_ENTITY_PARSE_CTE.format(alias=alias, table=table)
    return f"""
{parse},
nlp_tirads AS (
    SELECT
        research_id,
        TRUE AS nlp_tirads_has_data,
        COUNT(*) AS nlp_tirads_n_entities,
        COUNT(DISTINCT note_row_id) AS nlp_tirads_n_notes,
        MAX(CASE
            WHEN entity_value ILIKE '%TR5%' OR entity_value ILIKE '%TIRADS 5%' THEN 'TR5'
            WHEN entity_value ILIKE '%TR4%' OR entity_value ILIKE '%TIRADS 4%' THEN 'TR4'
            WHEN entity_value ILIKE '%TR3%' OR entity_value ILIKE '%TIRADS 3%' THEN 'TR3'
            WHEN entity_value ILIKE '%TR2%' OR entity_value ILIKE '%TIRADS 2%' THEN 'TR2'
            WHEN entity_value ILIKE '%TR1%' OR entity_value ILIKE '%TIRADS 1%' THEN 'TR1'
            ELSE entity_value
        END) AS nlp_tirads_max_category,
        BOOL_OR(entity_type ILIKE '%compos%' OR entity_type ILIKE '%echogen%') AS nlp_tirads_has_component_detail
    FROM {alias}_pos
    GROUP BY research_id
)"""


def tier1_cervical_ln_sql() -> str:
    alias = "cln"
    table = "note_entities_llm_cervical_ln_detail"
    parse = LLM_ENTITY_PARSE_CTE.format(alias=alias, table=table)
    return f"""
{parse},
nlp_cervln AS (
    SELECT
        research_id,
        TRUE AS nlp_ln_has_data,
        COUNT(*) AS nlp_ln_n_entities,
        COUNT(DISTINCT note_row_id) AS nlp_ln_n_notes,
        BOOL_OR(entity_value ILIKE '%positive%' OR entity_value ILIKE '%metasta%' OR entity_value ILIKE '%involved%') AS nlp_ln_positive_mentioned,
        STRING_AGG(DISTINCT
            CASE WHEN entity_type ILIKE '%level%' THEN entity_value END,
            ', '
        ) AS nlp_ln_levels_mentioned
    FROM {alias}_pos
    GROUP BY research_id
)"""


def tier1_tg_kinetics_sql() -> str:
    alias = "tgk"
    table = "note_entities_llm_tg_kinetics"
    parse = LLM_ENTITY_PARSE_CTE.format(alias=alias, table=table)
    return f"""
{parse},
nlp_tgkin AS (
    SELECT
        research_id,
        TRUE AS nlp_tg_has_data,
        COUNT(*) AS nlp_tg_n_entities,
        BOOL_OR(entity_value ILIKE '%rising%' OR entity_value ILIKE '%increas%' OR entity_value ILIKE '%elevat%') AS nlp_tg_rising_mentioned,
        BOOL_OR(entity_value ILIKE '%undetect%' OR entity_value ILIKE '%<0.%' OR entity_value ILIKE '%negative%') AS nlp_tg_undetectable_mentioned
    FROM {alias}_pos
    GROUP BY research_id
)"""


# -----------------------------------------------------------------------
# Tier 2 rollup SQL generators (flagged as lower concordance)
# -----------------------------------------------------------------------

def tier2_recurrence_sql() -> str:
    alias = "rec"
    table = "note_entities_llm_recurrence"
    parse = LLM_ENTITY_PARSE_CTE.format(alias=alias, table=table)
    return f"""
{parse},
nlp_recurrence AS (
    SELECT
        research_id,
        TRUE AS nlp_rec_has_data,
        COUNT(*) AS nlp_rec_n_entities,
        BOOL_OR(TRUE) AS nlp_rec_any_mentioned,
        MAX(CASE
            WHEN entity_value ILIKE '%distant%' OR entity_value ILIKE '%metasta%' THEN 'distant'
            WHEN entity_value ILIKE '%regional%' OR entity_value ILIKE '%lateral%' THEN 'regional'
            WHEN entity_value ILIKE '%local%' OR entity_value ILIKE '%central%' THEN 'local'
            ELSE 'unspecified'
        END) AS nlp_rec_type_worst,
        MIN(TRY_CAST(entity_date AS DATE)) AS nlp_rec_earliest_date,
        BOOL_OR(entity_value ILIKE '%disease free%' OR entity_value ILIKE '%no evidence%' OR entity_value ILIKE '%NED%') AS nlp_rec_disease_free_mentioned,
        'below_80pct_concordance' AS nlp_rec_confidence_tier
    FROM {alias}_pos
    GROUP BY research_id
)"""


def tier2_vascular_sql() -> str:
    alias = "vasc"
    table = "note_entities_llm_vascular_invasion"
    parse = LLM_ENTITY_PARSE_CTE.format(alias=alias, table=table)
    return f"""
{parse},
nlp_vascular AS (
    SELECT
        research_id,
        TRUE AS nlp_vasc_has_data,
        COUNT(*) AS nlp_vasc_n_entities,
        BOOL_OR(entity_value ILIKE '%positive%' OR entity_value ILIKE '%present%' OR entity_value ILIKE '%identified%') AS nlp_vasc_positive_mentioned,
        'below_80pct_concordance' AS nlp_vasc_confidence_tier
    FROM {alias}_pos
    GROUP BY research_id
)"""


# -----------------------------------------------------------------------
# Tier 3 — minimal rollup for remaining LLM domains
# -----------------------------------------------------------------------
TIER3_DOMAINS = [
    ("synoptic",    "note_entities_llm_synoptic_pathology_enrichment"),
    ("pmhx",        "note_entities_llm_past_medical_hx"),
    ("pshx",        "note_entities_llm_past_surgical_hx"),
    ("usnodule",    "note_entities_llm_us_nodule_dynamics"),
    ("symptoms",    "note_entities_llm_presenting_symptoms"),
    ("labs",        "note_entities_llm_labs"),
    ("physexam",    "note_entities_llm_physical_exam"),
    ("survfu",      "note_entities_llm_survival_followup"),
    ("parathyroid", "note_entities_llm_parathyroid_detail"),
    ("ptdecision",  "note_entities_llm_patient_decision_adherence"),
    ("funcoutcome", "note_entities_llm_functional_outcomes"),
    ("imaging",     "note_entities_llm_imaging"),
    ("airway",      "note_entities_llm_airway_invasion"),
    ("radtx",       "note_entities_llm_rad_treatment"),
    ("frozensec",   "note_entities_llm_frozen_section_detail"),
    ("dynrisk",     "note_entities_llm_dynamic_risk_response"),
    ("raidetail",   "note_entities_llm_rai_detailed"),
]


def tier3_domain_sql(short_name: str, table: str) -> str:
    alias = f"t3_{short_name}"
    parse = LLM_ENTITY_PARSE_CTE.format(alias=alias, table=table)
    return f"""
{parse},
nlp_{short_name} AS (
    SELECT
        research_id,
        TRUE AS nlp_{short_name}_has_data,
        COUNT(*) AS nlp_{short_name}_n_entities,
        COUNT(DISTINCT note_row_id) AS nlp_{short_name}_n_notes,
        MODE(entity_value) FILTER (WHERE confidence >= 0.7) AS nlp_{short_name}_key_finding
    FROM {alias}_pos
    GROUP BY research_id
)"""


# -----------------------------------------------------------------------
# Non-LLM entity tables — simple row count rollup
# -----------------------------------------------------------------------
NON_LLM_TABLES = [
    ("ne_complications",    "note_entities_complications"),
    ("ne_genetics",         "note_entities_genetics"),
    ("ne_medications",      "note_entities_medications"),
    ("ne_operative",        "note_entities_operative_detail"),
    ("ne_procedures",       "note_entities_procedures"),
    ("ne_problemlist",      "note_entities_problem_list"),
    ("ne_staging",          "note_entities_staging"),
]


def non_llm_rollup_sql(short_name: str, table: str) -> str:
    return f"""
nlp_{short_name} AS (
    SELECT
        CAST(research_id AS BIGINT) AS research_id,
        TRUE AS nlp_{short_name}_has_data,
        COUNT(*) AS nlp_{short_name}_n_rows
    FROM {table}
    GROUP BY CAST(research_id AS BIGINT)
)"""


def main():
    parser = argparse.ArgumentParser(description="Script 212: NLP entity rollup")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    token = get_token()
    con = duckdb.connect(f"md:{DB}?motherduck_token={token}")
    print(f"[212] Connected to {DB}")

    cur_cols = con.execute(f"""
        SELECT COUNT(DISTINCT column_name)
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchone()[0]
    cur_rows = con.execute(f"SELECT COUNT(*) FROM {CANONICAL}").fetchone()[0]
    print(f"[212] Current canonical: {cur_rows} rows, {cur_cols} columns")

    # Build all CTEs
    all_cte_blocks = []
    all_cte_names = []

    # Tier 1
    tier1_funcs = [
        ("nlp_pathology",  tier1_pathology_sql),
        ("nlp_tirads",     tier1_tirads_sql),
        ("nlp_cervln",     tier1_cervical_ln_sql),
        ("nlp_tgkin",      tier1_tg_kinetics_sql),
    ]
    for cte_name, fn in tier1_funcs:
        block = fn()
        all_cte_blocks.append(block)
        all_cte_names.append(cte_name)
        print(f"  [212] Built CTE: {cte_name} (Tier 1)")

    # Tier 2
    tier2_funcs = [
        ("nlp_recurrence", tier2_recurrence_sql),
        ("nlp_vascular",   tier2_vascular_sql),
    ]
    for cte_name, fn in tier2_funcs:
        block = fn()
        all_cte_blocks.append(block)
        all_cte_names.append(cte_name)
        print(f"  [212] Built CTE: {cte_name} (Tier 2)")

    # Tier 3
    for short_name, table in TIER3_DOMAINS:
        cte_name = f"nlp_{short_name}"
        block = tier3_domain_sql(short_name, table)
        all_cte_blocks.append(block)
        all_cte_names.append(cte_name)
    print(f"  [212] Built {len(TIER3_DOMAINS)} Tier 3 CTEs")

    # Non-LLM
    for short_name, table in NON_LLM_TABLES:
        cte_name = f"nlp_{short_name}"
        block = non_llm_rollup_sql(short_name, table)
        all_cte_blocks.append(block)
        all_cte_names.append(cte_name)
    print(f"  [212] Built {len(NON_LLM_TABLES)} non-LLM CTEs")

    print(f"[212] Total CTEs: {len(all_cte_names)}")

    # Due to MotherDuck CTE complexity limits, process in two batches:
    # Batch A: Tier 1+2 (6 domains, complex parsing)
    # Batch B: Tier 3 + non-LLM (24 domains, simpler)
    # We'll do two sequential ALTER TABLE ADD COLUMN ... approaches,
    # but since that's fragile, we'll do two staging table rebuilds.

    # Actually, for maximum reliability, do a single mega-CTE rebuild
    # but if that fails due to complexity, fall back to batched approach.

    # Assemble the full CTE chain
    # The LLM parse CTEs include multiple sub-CTEs (parsed, flat, ext, pos, final)
    # Non-LLM CTEs are self-contained
    # Tier 1/2/3 LLM CTEs each contribute 4 sub-CTEs + 1 rollup

    # Build join refs
    join_clauses = []
    select_refs = []
    for cte_name in all_cte_names:
        join_clauses.append(
            f"LEFT JOIN {cte_name} ON c.research_id = {cte_name}.research_id"
        )
        select_refs.append(f"{cte_name}.* EXCLUDE (research_id)")

    # Combine all CTE blocks. Each tier1/2/3 block starts with sub-CTEs
    # and ends with the rollup CTE. They're all comma-separated in WITH.
    combined_cte_sql = ",\n".join(all_cte_blocks)

    rebuild_sql = f"""
WITH
{combined_cte_sql}

SELECT
    c.*,
    {",".join(select_refs)}
FROM {CANONICAL} c
{chr(10).join(join_clauses)}
"""

    if args.dry_run:
        print(f"\n[212] DRY RUN — SQL length: {len(rebuild_sql)} chars")
        print(rebuild_sql[:4000])
        print("...[truncated]...")
        return

    # Execute in a single rebuild
    staging = f"{CANONICAL}_staging_212"
    t0 = time.time()

    try:
        print("[212] Executing mega-CTE rebuild (this may take a few minutes)...")
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        con.execute(f"CREATE TABLE {staging} AS {rebuild_sql}")
        elapsed = time.time() - t0
        print(f"[212] Staging table created in {elapsed:.1f}s")
    except Exception as e:
        print(f"[212] Mega-CTE failed: {e}")
        print("[212] Falling back to batched approach...")
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        _batched_rebuild(con, all_cte_blocks, all_cte_names, select_refs, join_clauses)
        return

    # Validate staging
    stg_rows = con.execute(f"SELECT COUNT(*) FROM {staging}").fetchone()[0]
    stg_rids = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {staging}").fetchone()[0]
    stg_null_rids = con.execute(
        f"SELECT COUNT(*) FROM {staging} WHERE research_id IS NULL"
    ).fetchone()[0]
    stg_dupes = stg_rows - stg_rids

    print(f"[212] Staging: {stg_rows} rows, {stg_rids} distinct RIDs, "
          f"{stg_null_rids} null RIDs, {stg_dupes} dupes")

    if stg_rows != cur_rows or stg_dupes > 0 or stg_null_rids > 0:
        print(f"[212] ERROR: Invariant failure. Aborting.")
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        sys.exit(1)

    # fna_path_outcome check
    fna_null = con.execute(
        f"SELECT COUNT(*) FROM {staging} WHERE fna_path_outcome IS NULL"
    ).fetchone()[0]
    print(f"[212] fna_path_outcome nulls: {fna_null}")

    # Swap
    con.execute(f"DROP TABLE IF EXISTS {CANONICAL}")
    con.execute(f"ALTER TABLE {staging} RENAME TO {CANONICAL}")

    new_cols = con.execute(f"""
        SELECT COUNT(DISTINCT column_name)
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchone()[0]
    added = new_cols - cur_cols
    print(f"[212] Columns: {cur_cols} → {new_cols} (+{added})")

    # Coverage summary
    print("\n[212] === NLP Coverage Summary ===")
    for cte_name in all_cte_names:
        has_col = f"{cte_name.replace('nlp_', 'nlp_')}_has_data"
        if "ne_" in cte_name:
            has_col = f"{cte_name}_has_data"
        try:
            cnt = con.execute(
                f"SELECT COUNT(*) FROM {CANONICAL} WHERE {has_col} IS TRUE"
            ).fetchone()[0]
            print(f"  {cte_name:30s}: {cnt:,} patients")
        except Exception:
            pass

    # Final invariant
    final = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               COUNT(*) FILTER (WHERE research_id IS NULL),
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL)
        FROM {CANONICAL}
    """).fetchone()
    print(f"\n[212] Final: {final[0]} rows, {final[1]} distinct, "
          f"{final[2]} null RIDs, {final[3]} null fna")

    if final[0] == 10871 and final[1] == 10871 and final[2] == 0:
        print("[212] All invariants PASS")
    else:
        print("[212] INVARIANT FAILURE")
        sys.exit(1)

    print("[212] Done.")


def _batched_rebuild(con, all_cte_blocks, all_cte_names, select_refs, join_clauses):
    """Fallback: add columns in two batches if mega-CTE is too complex."""
    cur_rows = con.execute(f"SELECT COUNT(*) FROM {CANONICAL}").fetchone()[0]

    # Split at the boundary between Tier 2 and Tier 3 (index 6)
    batch_boundary = 6
    batches = [
        ("A_tier12", all_cte_blocks[:batch_boundary], all_cte_names[:batch_boundary],
         select_refs[:batch_boundary], join_clauses[:batch_boundary]),
        ("B_tier3_ne", all_cte_blocks[batch_boundary:], all_cte_names[batch_boundary:],
         select_refs[batch_boundary:], join_clauses[batch_boundary:]),
    ]

    for batch_label, cte_blocks, cte_names, sel_refs, jn_clauses in batches:
        print(f"[212] Batch {batch_label}: {len(cte_names)} CTEs")
        staging = f"{CANONICAL}_staging_212_{batch_label}"
        combined = ",\n".join(cte_blocks)
        sql = f"""
WITH
{combined}

SELECT
    c.*,
    {",".join(sel_refs)}
FROM {CANONICAL} c
{chr(10).join(jn_clauses)}
"""
        t0 = time.time()
        con.execute(f"DROP TABLE IF EXISTS {staging}")
        con.execute(f"CREATE TABLE {staging} AS {sql}")
        elapsed = time.time() - t0

        stg_rows = con.execute(f"SELECT COUNT(*) FROM {staging}").fetchone()[0]
        stg_rids = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {staging}").fetchone()[0]
        print(f"[212] Batch {batch_label}: {stg_rows} rows, {stg_rids} RIDs in {elapsed:.1f}s")

        if stg_rows != cur_rows or stg_rows != stg_rids:
            print(f"[212] ERROR in batch {batch_label}. Aborting.")
            con.execute(f"DROP TABLE IF EXISTS {staging}")
            sys.exit(1)

        con.execute(f"DROP TABLE IF EXISTS {CANONICAL}")
        con.execute(f"ALTER TABLE {staging} RENAME TO {CANONICAL}")
        print(f"[212] Batch {batch_label} applied.")

    # Final col count
    new_cols = con.execute(f"""
        SELECT COUNT(DISTINCT column_name)
        FROM information_schema.columns
        WHERE table_name = '{CANONICAL}' AND table_schema = 'main'
    """).fetchone()[0]
    print(f"[212] Final column count: {new_cols}")

    final = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               COUNT(*) FILTER (WHERE research_id IS NULL),
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL)
        FROM {CANONICAL}
    """).fetchone()
    print(f"[212] Final: {final[0]} rows, {final[1]} distinct, "
          f"{final[2]} null RIDs, {final[3]} null fna")

    if final[0] == 10871 and final[1] == 10871 and final[2] == 0:
        print("[212] All invariants PASS")
    else:
        print("[212] INVARIANT FAILURE")
        sys.exit(1)

    print("[212] Done (batched).")


if __name__ == "__main__":
    main()
