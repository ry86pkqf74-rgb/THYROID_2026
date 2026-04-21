#!/usr/bin/env python3
"""
Script runpod_404 — esophageal_invasion extraction upload + CPM rollup (NEW).

Job 3 of the 2026-04-21 RunPod round. Loads the FIRST-EVER
main.note_entities_llm_esophageal_invasion table from the qwen2.5-32b
extraction against OPNOTE-only corpus (4,409 OP notes), and populates:

  * op_esophageal_inv_* columns on canonical_patient_master (already exist
    per preflight audit; currently all NULL)
  * op_nlp_esophageal_involvement / op_nlp_esophageal_n_mentions

Critical coordination note (from preflight):
  Script 342 of the Cursor Prompt 5 batch will fill op_esophageal_inv_any
  from an airway-JSON proxy before this script runs. This script's Phase
  5 OVERWRITES op_esophageal_inv_any with real extraction results — this
  overwrite is INTENTIONAL per the handoff ("Job 3 overwrites it with
  real extraction results later").

Phases:
  0   Audit output parquet
  1   (Skipped — table does not yet exist, nothing to archive)
  2   Load parquet to main.note_entities_llm_esophageal_invasion (NEW)
  3   Parity-check MD vs parquet
  4   CPM snapshot
  5   Rebuild op_esophageal_inv_* + op_nlp_esophageal_* on CPM
  6   Post-mutation invariants + registry stub

Target CPM columns (8):
    op_esophageal_inv_any                 BOOLEAN  (TRUE iff >=1 present entity)
    op_esophageal_inv_first_date          DATE     (MIN entity_date per rid)
    op_esophageal_inv_first_evidence_text VARCHAR  (earliest evidence_text)
    op_esophageal_inv_first_source_note_ref VARCHAR (earliest note_row_id)
    op_esophageal_inv_n_notes_documenting BIGINT   (distinct note_row_ids)
    op_esophageal_inv_source_table        VARCHAR  (literal 'note_entities_llm_esophageal_invasion')
    op_nlp_esophageal_involvement         BOOLEAN  (TRUE iff has_data)
    op_nlp_esophageal_n_mentions          BIGINT   (count of present entities)

Hard rules (same as Script 285):
  * Auth via motherduck_client.get_token()
  * CAST(research_id AS VARCHAR) on all CPM joins
  * archive DB literal name is "Thyroid 2026 UPdated" (with space)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.runpod_round2._helper import (  # noqa: E402
    audit_parquet,
    connect,
    load_parquet_to_md,
    log,
    parity_check_md_vs_parquet,
    snapshot_cpm,
    table_exists,
    write_summary,
)

DOMAIN = "esophageal_invasion"
SOURCE_TABLE = "note_entities_llm_esophageal_invasion"
SOURCE_TABLE_LITERAL = "note_entities_llm_esophageal_invasion"

PARQUET_PATH = (
    REPO_ROOT
    / "runs"
    / "round2_20260421"
    / DOMAIN
    / "output"
    / f"note_entities_llm_{DOMAIN}.parquet"
)

# Corpus shape bounds — OPNOTE-only, ~4,409 notes per build_input_parquets_round2.
EXPECTED_ROWS_MIN = 3_800
EXPECTED_ROWS_MAX = 5_000
EXPECTED_RIDS_MIN = 3_500
EXPECTED_RIDS_MAX = 4_500

# Entity types from esophageal_invasion_extraction_v1.txt.
ENTITY_TYPES_KNOWN = (
    "esophageal_invasion_present",
    "esophageal_invasion_extent",
    "esophageal_invasion_length_cm",
    "esophageal_repair_performed",
    "esophageal_muscularis_invasion",
    "esophageal_mucosal_invasion",
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
           json_extract_string(entity, '$.entity_type')     AS entity_type,
           json_extract_string(entity, '$.entity_value')    AS entity_value,
           json_extract_string(entity, '$.entity_date')     AS entity_date_str,
           json_extract_string(entity, '$.evidence_text')   AS evidence_text,
           json_extract_string(entity, '$.present_or_negated') AS pre_neg,
           COALESCE(TRY_CAST(json_extract(entity, '$.confidence') AS DOUBLE), 0) AS confidence
      FROM flat
     WHERE json_extract_string(entity, '$.entity_value') IS NOT NULL
),
pos AS (
    SELECT * FROM ext
     WHERE confidence >= 0.5
       AND (pre_neg = 'present' OR pre_neg IS NULL)
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY research_id
               ORDER BY TRY_CAST(entity_date_str AS DATE) NULLS LAST,
                        note_row_id
           ) AS rn_earliest
      FROM pos
),
agg AS (
    SELECT
        CAST(research_id AS VARCHAR)                                 AS rid_v,
        TRUE                                                         AS op_esophageal_inv_any,
        MIN(TRY_CAST(entity_date_str AS DATE))                       AS op_esophageal_inv_first_date,
        MAX(CASE WHEN rn_earliest = 1 THEN evidence_text END)        AS op_esophageal_inv_first_evidence_text,
        MAX(CASE WHEN rn_earliest = 1 THEN note_row_id END)          AS op_esophageal_inv_first_source_note_ref,
        COUNT(DISTINCT note_row_id)::BIGINT                          AS op_esophageal_inv_n_notes_documenting,
        '{SOURCE_TABLE_LITERAL}'                                     AS op_esophageal_inv_source_table,
        TRUE                                                         AS op_nlp_esophageal_involvement,
        COUNT(*)::BIGINT                                             AS op_nlp_esophageal_n_mentions
      FROM ranked
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
    return info


def phase1_archive(con) -> dict:
    log("=== Phase 1 — archive (NEW table; skip if absent) ===")
    exists = table_exists(con, "main", SOURCE_TABLE)
    log(f"  main.{SOURCE_TABLE} exists? {exists}")
    return {"pre_existed": exists}


def phase2_load(con) -> int:
    log(f"=== Phase 2 — load parquet to main.{SOURCE_TABLE} ===")
    return load_parquet_to_md(con, PARQUET_PATH, SOURCE_TABLE, DOMAIN)


def phase3_parity(con) -> dict:
    log("=== Phase 3 — MD ↔ parquet parity ===")
    res = parity_check_md_vs_parquet(con, SOURCE_TABLE, PARQUET_PATH)
    if not res["ok"]:
        raise SystemExit(f"FAIL: parity mismatch {res}")
    return res


def phase4_snapshot_cpm(con) -> str:
    return snapshot_cpm(con, "pre_op_esophageal")


def phase5_rollup(con) -> dict:
    log("=== Phase 5 — rebuild op_esophageal_inv_* on CPM ===")
    # Zero-out pass: set rollup columns to FALSE/0/NULL for all CPM rows.
    # CRITICAL — this overwrites any airway-JSON-proxy fill from Script 342.
    con.execute(
        """
        UPDATE main.canonical_patient_master
           SET op_esophageal_inv_any                 = FALSE,
               op_esophageal_inv_first_date          = NULL,
               op_esophageal_inv_first_evidence_text = NULL,
               op_esophageal_inv_first_source_note_ref = NULL,
               op_esophageal_inv_n_notes_documenting = 0,
               op_esophageal_inv_source_table        = NULL,
               op_nlp_esophageal_involvement         = FALSE,
               op_nlp_esophageal_n_mentions          = 0;
        """
    )
    con.execute(
        f"""
        WITH {ROLLUP_BASE_CTE}
        UPDATE main.canonical_patient_master AS c
           SET op_esophageal_inv_any                 = a.op_esophageal_inv_any,
               op_esophageal_inv_first_date          = a.op_esophageal_inv_first_date,
               op_esophageal_inv_first_evidence_text = a.op_esophageal_inv_first_evidence_text,
               op_esophageal_inv_first_source_note_ref = a.op_esophageal_inv_first_source_note_ref,
               op_esophageal_inv_n_notes_documenting = a.op_esophageal_inv_n_notes_documenting,
               op_esophageal_inv_source_table        = a.op_esophageal_inv_source_table,
               op_nlp_esophageal_involvement         = a.op_nlp_esophageal_involvement,
               op_nlp_esophageal_n_mentions          = a.op_nlp_esophageal_n_mentions
          FROM agg AS a
         WHERE CAST(c.research_id AS VARCHAR) = a.rid_v;
        """
    )
    stats = con.execute(
        """
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE op_esophageal_inv_any)                AS any_true,
               COUNT(*) FILTER (WHERE op_nlp_esophageal_involvement)        AS nlp_true,
               MIN(op_esophageal_inv_first_date)                            AS earliest_date,
               MAX(op_esophageal_inv_first_date)                            AS latest_date,
               SUM(op_nlp_esophageal_n_mentions)                            AS sum_mentions
          FROM main.canonical_patient_master;
        """
    ).fetchone()
    out = {
        "cpm_rows": int(stats[0]),
        "op_esophageal_inv_any_true": int(stats[1]),
        "op_nlp_esophageal_involvement_true": int(stats[2]),
        "earliest_date": str(stats[3]),
        "latest_date": str(stats[4]),
        "sum_mentions": int(stats[5] or 0),
    }
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
        "WHERE op_esophageal_inv_any IS NULL"
    ).fetchone()[0]
    if nulls:
        raise SystemExit(f"FAIL: {nulls} NULL op_esophageal_inv_any")
    # Source-table label sanity.
    bad_label = con.execute(
        f"""
        SELECT COUNT(*) FROM main.canonical_patient_master
         WHERE op_esophageal_inv_any = TRUE
           AND op_esophageal_inv_source_table != '{SOURCE_TABLE_LITERAL}';
        """
    ).fetchone()[0]
    if bad_label:
        raise SystemExit(f"FAIL: {bad_label} rows with any=TRUE but wrong source_table")
    log("  invariants OK")
    return {"cpm_rows": int(row[0]), "nulls": 0, "bad_source_labels": int(bad_label)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", default="0", help="0,1,2,3,4,5,6,all")
    args = ap.parse_args()
    phases = ["0","1","2","3","4","5","6"] if args.phase == "all" else [args.phase]

    summary: dict = {"domain": DOMAIN, "phases_run": phases}
    summary["phase0"] = phase0_audit()
    if phases != ["0"]:
        con = connect()
        if "1" in phases: summary["phase1"] = phase1_archive(con)
        if "2" in phases: summary["phase2"] = phase2_load(con)
        if "3" in phases: summary["phase3"] = phase3_parity(con)
        if "4" in phases: summary["phase4"] = phase4_snapshot_cpm(con)
        if "5" in phases: summary["phase5"] = phase5_rollup(con)
        if "6" in phases: summary["phase6"] = phase6_invariants(con)
        con.close()

    write_summary("runpod_404_esophageal_invasion", summary)
    log("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
