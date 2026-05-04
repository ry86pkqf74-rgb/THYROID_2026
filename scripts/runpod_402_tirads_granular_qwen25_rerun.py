#!/usr/bin/env python3
"""
Script runpod_402 — tirads_granular qwen2.5-32b re-extraction upload + CPM rollup.

Replaces stale qwen3:32b (2026-03-31) extraction of
main.note_entities_llm_tirads_granular with the 2026-04-21 qwen2.5-32b
output produced on RunPod pod pmza5juk7ru2xl.

Prompt: llm_extraction/prompts/tirads_granular_extraction_v2.txt (v2 — already
iterated past v1 per the preflight audit; this script loads the new extractor
output without changing the prompt version).

CPM columns rebuilt (nlp_tirads_* — 4 cols + tirads_resolved merge for NLP TR1–TR5):
    nlp_tirads_has_data
    nlp_tirads_n_entities
    nlp_tirads_n_notes
    nlp_tirads_has_component_detail
    tirads_resolved (COALESCE(nlp_clean_tr, existing) — legacy nlp_tirads_max_category dropped mig_294b)

NOTE: The Job 2 (TIRADS requeue) pathway in runpod_403 is NOT a subset of
this job. Job 2 re-runs the v2 prompt over a nodule-level queue
(tirads_reextraction_queue_v1, 4,363 nodules) with a different output
table; Job 1c here is a full note-level re-extraction with the same scope
as the original qwen3 run.

Phases match Script runpod_400 (0 = audit, all = 0..6).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.runpod_round2._helper import (  # noqa: E402
    archive_current_table,
    audit_parquet,
    connect,
    load_parquet_to_md,
    log,
    parity_check_md_vs_parquet,
    snapshot_cpm,
    write_summary,
)

DOMAIN = "tirads_granular"
SOURCE_TABLE = "note_entities_llm_tirads_granular"
PARQUET_PATH = (
    REPO_ROOT
    / "runs"
    / "round2_20260421"
    / DOMAIN
    / "output"
    / f"note_entities_llm_{DOMAIN}.parquet"
)

EXPECTED_ROWS_MIN = 9_500
EXPECTED_ROWS_MAX = 12_000
EXPECTED_RIDS_MIN = 4_800
EXPECTED_RIDS_MAX = 5_800

CPM_COLS = (
    "nlp_tirads_has_data",
    "nlp_tirads_n_entities",
    "nlp_tirads_n_notes",
    "nlp_tirads_has_component_detail",
    "tirads_resolved",
)

ROLLUP_BASE_CTE = f"""
parsed AS (
    SELECT research_id, note_row_id,
           json_extract(CAST(result_json AS JSON), '$.entities') AS entities_arr
      FROM main.{SOURCE_TABLE}
     WHERE result_json IS NOT NULL
       AND CAST(result_json AS VARCHAR) NOT LIKE '%"entities": []%'
       AND json_type(json_extract(CAST(result_json AS JSON), '$.entities')) = 'ARRAY'
),
flat AS (
    SELECT research_id, note_row_id,
           UNNEST(CAST(entities_arr AS JSON[])) AS entity
      FROM parsed
),
ext AS (
    SELECT research_id, note_row_id,
           json_extract_string(entity, '$.entity_type')  AS entity_type,
           json_extract_string(entity, '$.entity_value') AS entity_value,
           COALESCE(TRY_CAST(json_extract(entity, '$.confidence') AS DOUBLE), 0) AS confidence,
           json_extract_string(entity, '$.present_or_negated') AS pre_neg
      FROM flat
     WHERE json_extract_string(entity, '$.entity_value') IS NOT NULL
),
pos AS (
    SELECT * FROM ext
     WHERE confidence >= 0.5
       AND (pre_neg = 'present' OR pre_neg IS NULL)
),
agg AS (
    SELECT
        CAST(research_id AS VARCHAR) AS rid_v,
        TRUE                                                             AS nlp_tirads_has_data,
        COUNT(*)::BIGINT                                                 AS nlp_tirads_n_entities,
        COUNT(DISTINCT note_row_id)::BIGINT                              AS nlp_tirads_n_notes,
        MAX(CASE
            WHEN entity_value ILIKE '%TR5%' OR entity_value ILIKE '%TIRADS 5%' THEN 'TR5'
            WHEN entity_value ILIKE '%TR4%' OR entity_value ILIKE '%TIRADS 4%' THEN 'TR4'
            WHEN entity_value ILIKE '%TR3%' OR entity_value ILIKE '%TIRADS 3%' THEN 'TR3'
            WHEN entity_value ILIKE '%TR2%' OR entity_value ILIKE '%TIRADS 2%' THEN 'TR2'
            WHEN entity_value ILIKE '%TR1%' OR entity_value ILIKE '%TIRADS 1%' THEN 'TR1'
            ELSE NULL
        END)                                                             AS nlp_tirads_clean_tr,
        BOOL_OR(entity_type ILIKE '%compos%' OR entity_type ILIKE '%echogen%'
                OR entity_type ILIKE '%calcif%' OR entity_type ILIKE '%shape%'
                OR entity_type ILIKE '%margin%')                          AS nlp_tirads_has_component_detail
      FROM pos
     GROUP BY 1
)
"""


def phase0_audit() -> dict:
    log("=== Phase 0 — audit parquet ===")
    if not PARQUET_PATH.exists():
        raise SystemExit(f"Parquet not found: {PARQUET_PATH}")
    info = audit_parquet(PARQUET_PATH)
    log(f"  {info}")
    if not (EXPECTED_ROWS_MIN <= info["rows"] <= EXPECTED_ROWS_MAX):
        raise SystemExit(f"FAIL: rows={info['rows']}")
    if not (EXPECTED_RIDS_MIN <= info["rids"] <= EXPECTED_RIDS_MAX):
        raise SystemExit(f"FAIL: rids={info['rids']}")
    return info


def phase5_rollup(con) -> dict:
    log("=== Phase 5 — rebuild nlp_tirads_* on CPM ===")
    con.execute(
        """
        UPDATE main.canonical_patient_master
           SET nlp_tirads_has_data             = FALSE,
               nlp_tirads_n_entities           = 0,
               nlp_tirads_n_notes              = 0,
               nlp_tirads_has_component_detail = FALSE;
        """
    )
    con.execute(
        f"""
        WITH {ROLLUP_BASE_CTE}
        UPDATE main.canonical_patient_master AS c
           SET nlp_tirads_has_data             = a.nlp_tirads_has_data,
               nlp_tirads_n_entities           = a.nlp_tirads_n_entities,
               nlp_tirads_n_notes              = a.nlp_tirads_n_notes,
               nlp_tirads_has_component_detail = a.nlp_tirads_has_component_detail,
               tirads_resolved                 = COALESCE(a.nlp_tirads_clean_tr, c.tirads_resolved)
          FROM agg AS a
         WHERE CAST(c.research_id AS VARCHAR) = a.rid_v;
        """
    )
    stats = con.execute(
        """
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE nlp_tirads_has_data),
               COUNT(DISTINCT tirads_resolved)
          FROM main.canonical_patient_master;
        """
    ).fetchone()
    out = {"cpm_rows": int(stats[0]), "has_data_true": int(stats[1]),
           "tirads_resolved_distinct": int(stats[2])}
    log(f"  rollup: {out}")
    return out


def phase6_invariants(con) -> dict:
    log("=== Phase 6 — post-mutation invariants ===")
    row = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master;"
    ).fetchone()
    if row[0] != 10_871 or row[1] != 10_871:
        raise SystemExit(f"FAIL: CPM shape rows={row[0]} rids={row[1]}")
    nulls = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE nlp_tirads_has_data IS NULL"
    ).fetchone()[0]
    if nulls:
        raise SystemExit(f"FAIL: {nulls} NULL nlp_tirads_has_data")
    log("  invariants OK")
    return {"cpm_rows": int(row[0]), "nulls": 0}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", default="0", help="0,1,2,3,4,5,6,all")
    args = ap.parse_args()
    phases = ["0","1","2","3","4","5","6"] if args.phase == "all" else [args.phase]

    summary: dict = {"domain": DOMAIN, "phases_run": phases}
    summary["phase0"] = phase0_audit()
    if phases != ["0"]:
        con = connect()
        if "1" in phases: summary["phase1"] = archive_current_table(con, SOURCE_TABLE)
        if "2" in phases: summary["phase2"] = load_parquet_to_md(con, PARQUET_PATH, SOURCE_TABLE, DOMAIN)
        if "3" in phases:
            r = parity_check_md_vs_parquet(con, SOURCE_TABLE, PARQUET_PATH)
            if not r["ok"]: raise SystemExit(f"FAIL: parity {r}")
            summary["phase3"] = r
        if "4" in phases: summary["phase4"] = snapshot_cpm(con, "pre_nlp_tirads")
        if "5" in phases: summary["phase5"] = phase5_rollup(con)
        if "6" in phases: summary["phase6"] = phase6_invariants(con)
        con.close()

    write_summary("runpod_402_tirads_granular", summary)
    log("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
