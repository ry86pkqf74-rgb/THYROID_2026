#!/usr/bin/env python3
"""
Script runpod_400 — pathology qwen2.5-32b re-extraction upload + CPM rollup.

Replaces stale qwen3:32b (2026-03-30) extraction of
main.note_entities_llm_pathology with the 2026-04-21 qwen2.5-32b output
produced on RunPod pod pmza5juk7ru2xl.

Phases (CLI gate via --phase N or --phase all; default: audit only):
  0  Audit output parquet (rows, rids, dup note_row_ids) — READ-ONLY
  1  Archive current main.note_entities_llm_pathology to archive_pub_v1_0
  2  Load parquet to main.note_entities_llm_pathology (23-col sibling schema)
  3  Parity-check MD vs parquet (rows + note_row_id set)
  4  CPM snapshot to archive_pub_v1_0
  5  Rollup UPDATE — rebuild nlp_path_* columns on canonical_patient_master
  6  Post-mutation invariants (CPM row count, has_data counts)
  all  Run 0→6 in order, halting on failure

CPM columns rebuilt (all 8):
    nlp_path_has_data
    nlp_path_n_entities
    nlp_path_n_notes
    nlp_path_ete_mentioned
    nlp_path_histology_mentioned
    nlp_path_ln_positive_mentioned
    nlp_path_margin_mentioned
    nlp_path_multifocal_mentioned
    nlp_path_vasc_inv_mentioned

Hard rules (same as Script 285):
  * Auth via motherduck_client.get_token()
  * Always CAST(research_id AS VARCHAR) on join to CPM
  * Inline-escape single quotes in SQL literals; no ? placeholders inside
    COMMENTs or DDL
  * archive_pub_v1_0 lives in database "Thyroid 2026 UPdated" (literal space)
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

DOMAIN = "pathology"
SOURCE_TABLE = "note_entities_llm_pathology"
PARQUET_PATH = (
    REPO_ROOT
    / "runs"
    / "round2_20260421"
    / DOMAIN
    / "output"
    / f"note_entities_llm_{DOMAIN}.parquet"
)

# Expected shape (from preflight / historical qwen3 run — the qwen2.5 rerun
# should land within +/-10% of these).
EXPECTED_ROWS_MIN = 9_500
EXPECTED_ROWS_MAX = 12_000
EXPECTED_RIDS_MIN = 4_800
EXPECTED_RIDS_MAX = 5_800

CPM_COLS = (
    "nlp_path_has_data",
    "nlp_path_n_entities",
    "nlp_path_n_notes",
    "nlp_path_ete_mentioned",
    "nlp_path_histology_mentioned",
    "nlp_path_ln_positive_mentioned",
    "nlp_path_margin_mentioned",
    "nlp_path_multifocal_mentioned",
    "nlp_path_vasc_inv_mentioned",
)

# Base CTE parses result_json -> entities and filters to positive,
# confidence >= 0.5. Mirrors Script 212 LLM_ENTITY_PARSE_CTE / pos filter.
ROLLUP_BASE_CTE = f"""
parsed AS (
    SELECT
        research_id,
        note_row_id,
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
        TRUE                                                                                  AS nlp_path_has_data,
        COUNT(*)::BIGINT                                                                      AS nlp_path_n_entities,
        COUNT(DISTINCT note_row_id)::BIGINT                                                   AS nlp_path_n_notes,
        BOOL_OR(entity_type ILIKE '%ete%'       OR entity_type ILIKE '%extrathyroid%'
                OR entity_value ILIKE '%extrathyroid%')                                       AS nlp_path_ete_mentioned,
        MODE(entity_value) FILTER (WHERE entity_type ILIKE '%histolog%'
                                   OR entity_type ILIKE '%surgical_path%')                    AS nlp_path_histology_mentioned,
        BOOL_OR(entity_type ILIKE '%lymph_node%' OR entity_value ILIKE '%lymph node positive%'
                OR entity_value ILIKE '%metasta%')                                            AS nlp_path_ln_positive_mentioned,
        BOOL_OR(entity_type ILIKE '%margin%'    OR entity_value ILIKE '%margin%')             AS nlp_path_margin_mentioned,
        BOOL_OR(entity_type ILIKE '%multifocal%' OR entity_value ILIKE '%multifocal%')        AS nlp_path_multifocal_mentioned,
        BOOL_OR(entity_type ILIKE '%vascular%'  OR entity_type ILIKE '%angioinvasion%'
                OR entity_value ILIKE '%vascular invasion%')                                  AS nlp_path_vasc_inv_mentioned
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
        raise SystemExit(f"FAIL: rows={info['rows']} outside [{EXPECTED_ROWS_MIN},{EXPECTED_ROWS_MAX}]")
    if not (EXPECTED_RIDS_MIN <= info["rids"] <= EXPECTED_RIDS_MAX):
        raise SystemExit(f"FAIL: rids={info['rids']} outside [{EXPECTED_RIDS_MIN},{EXPECTED_RIDS_MAX}]")
    if info["dup_note_row_id_groups"] != 0:
        log(f"  WARN: {info['dup_note_row_id_groups']} dup note_row_id groups (dedup in Phase 2)")
    return info


def phase1_archive(con) -> str | None:
    log("=== Phase 1 — archive current table ===")
    return archive_current_table(con, SOURCE_TABLE)


def phase2_load(con) -> int:
    log("=== Phase 2 — load parquet to MD ===")
    return load_parquet_to_md(con, PARQUET_PATH, SOURCE_TABLE, DOMAIN)


def phase3_parity(con) -> dict:
    log("=== Phase 3 — MD ↔ parquet parity ===")
    res = parity_check_md_vs_parquet(con, SOURCE_TABLE, PARQUET_PATH)
    if not res["ok"]:
        raise SystemExit(f"FAIL: parity mismatch {res}")
    return res


def phase4_snapshot_cpm(con) -> str:
    log("=== Phase 4 — CPM snapshot ===")
    return snapshot_cpm(con, "pre_nlp_path")


def phase5_rollup(con) -> dict:
    log("=== Phase 5 — rebuild nlp_path_* on CPM ===")
    # Zero-out pass: set all 9 columns to FALSE/0/NULL for every CPM row.
    con.execute(
        """
        UPDATE main.canonical_patient_master
           SET nlp_path_has_data            = FALSE,
               nlp_path_n_entities          = 0,
               nlp_path_n_notes             = 0,
               nlp_path_ete_mentioned       = FALSE,
               nlp_path_histology_mentioned = NULL,
               nlp_path_ln_positive_mentioned = FALSE,
               nlp_path_margin_mentioned    = FALSE,
               nlp_path_multifocal_mentioned = FALSE,
               nlp_path_vasc_inv_mentioned  = FALSE;
        """
    )
    # Fill pass: join on rid and overwrite.
    con.execute(
        f"""
        WITH {ROLLUP_BASE_CTE}
        UPDATE main.canonical_patient_master AS c
           SET nlp_path_has_data            = a.nlp_path_has_data,
               nlp_path_n_entities          = a.nlp_path_n_entities,
               nlp_path_n_notes             = a.nlp_path_n_notes,
               nlp_path_ete_mentioned       = a.nlp_path_ete_mentioned,
               nlp_path_histology_mentioned = a.nlp_path_histology_mentioned,
               nlp_path_ln_positive_mentioned = a.nlp_path_ln_positive_mentioned,
               nlp_path_margin_mentioned    = a.nlp_path_margin_mentioned,
               nlp_path_multifocal_mentioned = a.nlp_path_multifocal_mentioned,
               nlp_path_vasc_inv_mentioned  = a.nlp_path_vasc_inv_mentioned
          FROM agg AS a
         WHERE CAST(c.research_id AS VARCHAR) = a.rid_v;
        """
    )
    stats = con.execute(
        """
        SELECT
          COUNT(*)                                            AS cpm_rows,
          COUNT(*) FILTER (WHERE nlp_path_has_data)           AS has_data_true,
          COUNT(*) FILTER (WHERE NOT nlp_path_has_data)       AS has_data_false,
          COUNT(*) FILTER (WHERE nlp_path_ete_mentioned)      AS ete_true,
          COUNT(*) FILTER (WHERE nlp_path_vasc_inv_mentioned) AS vasc_true
        FROM main.canonical_patient_master;
        """
    ).fetchone()
    out = {
        "cpm_rows": int(stats[0]),
        "has_data_true": int(stats[1]),
        "has_data_false": int(stats[2]),
        "ete_true": int(stats[3]),
        "vasc_true": int(stats[4]),
    }
    log(f"  rollup: {out}")
    return out


def phase6_invariants(con) -> dict:
    log("=== Phase 6 — post-mutation invariants ===")
    row = con.execute(
        """
        SELECT COUNT(*), COUNT(DISTINCT research_id)
          FROM main.canonical_patient_master;
        """
    ).fetchone()
    cpm_rows, cpm_rids = int(row[0]), int(row[1])
    if cpm_rows != 10_871 or cpm_rids != 10_871:
        raise SystemExit(f"FAIL: CPM shape changed rows={cpm_rows} rids={cpm_rids}")
    # has_data_true + has_data_false must cover all CPM rows (no NULLs).
    nulls = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE nlp_path_has_data IS NULL"
    ).fetchone()[0]
    if nulls:
        raise SystemExit(f"FAIL: {nulls} NULL nlp_path_has_data rows")
    log(f"  invariants OK: cpm_rows={cpm_rows} nulls=0")
    return {"cpm_rows": cpm_rows, "cpm_rids": cpm_rids, "nulls": 0}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", default="0", help="0,1,2,3,4,5,6,all")
    args = ap.parse_args()

    phases = (
        ["0", "1", "2", "3", "4", "5", "6"]
        if args.phase == "all"
        else [args.phase]
    )

    summary: dict = {"domain": DOMAIN, "phases_run": phases}

    # Phase 0 is read-only — no MD connection needed unless later phases requested.
    if phases == ["0"]:
        summary["phase0"] = phase0_audit()
    else:
        summary["phase0"] = phase0_audit()
        con = connect()
        if "1" in phases:
            summary["phase1"] = phase1_archive(con)
        if "2" in phases:
            summary["phase2"] = phase2_load(con)
        if "3" in phases:
            summary["phase3"] = phase3_parity(con)
        if "4" in phases:
            summary["phase4"] = phase4_snapshot_cpm(con)
        if "5" in phases:
            summary["phase5"] = phase5_rollup(con)
        if "6" in phases:
            summary["phase6"] = phase6_invariants(con)
        con.close()

    write_summary("runpod_400_pathology", summary)
    log("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
