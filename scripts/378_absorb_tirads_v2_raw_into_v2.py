#!/usr/bin/env python3
"""Script 378 — Phase 3: absorb tirads_v2_nodules_raw into canonical_us_nodule_v2.

Two cohorts in raw:
  * 5,082 rows at nodule_index_within_exam = 0 / 3,021 patients — most have
    real US data (composition, tirads_category, size_cm_max). v2 inherits
    cunc's 1-based indexing, so these have no slot today.
  * 6,832 rows at nodule_index >= 1 — should already be merged in v2 via
    Script 362; verify via field-level match and log mismatches.

Per-row decision tree for index=0 rows:
  Case A — no v2 row exists for that (research_id, exam_date):
            INSERT new v2 row with nodule_index=1.
  Case B — v2 has row(s) for that (research_id, exam_date):
            COALESCE UPDATE v2's existing row(s) with raw values where v2
            is NULL. Conflicts (raw non-null AND v2 non-null AND different)
            logged to manuscript_workspace.us_raw_index0_conflict_v1.

For nodule_index >= 1 rows: log mismatches to
manuscript_workspace.us_raw_index_mismatch_v1.

us_exam_id derivation:
  - If v2 already has a row for (rid, date), reuse that us_exam_id.
  - Otherwise compute md5(rid||'|'||date) (matching gland/LN v2 recipe).
"""
from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

PUB = PUBLICATION_DB
SCRIPT_TAG = "Script 378"
V2 = f"{PUB}.main.canonical_us_nodule_v2"
RAW = f"{PUB}.main.tirads_v2_nodules_raw"
CONFLICT_TABLE = f"{PUB}.manuscript_workspace.us_raw_index0_conflict_v1"
MISMATCH_TABLE = f"{PUB}.manuscript_workspace.us_raw_index_mismatch_v1"

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"378_absorb_raw_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    v2_before = con.execute(f"SELECT COUNT(*) FROM {V2}").fetchone()[0]
    log(f"  v2 baseline: {v2_before:,} rows")

    # Stage parsed raw rows (typed)
    con.execute(f"""
CREATE OR REPLACE TEMP TABLE _raw_typed AS
SELECT
    TRY_CAST(research_id AS INTEGER) AS research_id,
    TRY_CAST(linkage_date AS DATE)   AS exam_date,
    nodule_index_within_exam         AS raw_index,
    laterality, composition, echogenicity, shape, margin AS margins,
    halo, vascularity, extrathyroidal_extension_on_us,
    chammas_type, elastography AS elastography_category,
    tirads_category, size_cm_max,
    interval_growth_flag, prior_size_mm_max,
    fna_recommended_this_nodule, fna_performed_prior_or_concurrent,
    comparison_statement, evidence_text,
    -- echogenic_foci is VARCHAR[] in raw; collapse to scalar for v2 VARCHAR
    ARRAY_TO_STRING(echogenic_foci, '|') AS echogenic_foci
FROM {RAW}
WHERE TRY_CAST(research_id AS INTEGER) IS NOT NULL
""")

    n_raw = con.execute("SELECT COUNT(*) FROM _raw_typed").fetchone()[0]
    n_idx0 = con.execute(
        "SELECT COUNT(*) FROM _raw_typed WHERE raw_index = 0"
    ).fetchone()[0]
    n_idxN = con.execute(
        "SELECT COUNT(*) FROM _raw_typed WHERE raw_index >= 1"
    ).fetchone()[0]
    log(f"  raw rows total={n_raw:,}  index=0={n_idx0:,}  index≥1={n_idxN:,}")

    # ── Index=0: classify per-row Case A vs B ────────────────────────────────
    log("classify index=0 rows: Case A (no v2 for date) vs Case B (existing v2)")
    con.execute(f"""
CREATE OR REPLACE TEMP TABLE _raw0_with_class AS
SELECT
    r.*,
    CASE WHEN v_exists.us_exam_id IS NULL THEN 'A_insert'
         ELSE 'B_coalesce' END AS classification,
    v_exists.us_exam_id AS existing_us_exam_id
FROM (
    SELECT * FROM _raw_typed WHERE raw_index = 0
) r
LEFT JOIN (
    SELECT DISTINCT research_id, exam_date, ANY_VALUE(us_exam_id) AS us_exam_id
    FROM {V2} GROUP BY 1,2
) v_exists USING (research_id, exam_date)
""")

    counts = dict(con.execute(
        "SELECT classification, COUNT(*) FROM _raw0_with_class GROUP BY 1"
    ).fetchall())
    log(f"  index=0 classification: {counts}")

    if not args.commit:
        log("dry-run only.")
        return 0

    # ── Case A: INSERT new v2 rows (one per raw row, nodule_index=1) ────
    # If multiple raw index=0 rows exist for same (rid,date), pick the
    # first via QUALIFY ROW_NUMBER to avoid unique-key collisions.
    log("Case A INSERTs (one v2 row per raw index=0 group)")
    con.execute(f"""
INSERT INTO {V2} (
    research_id, us_exam_id, exam_date, nodule_index_within_exam, nodule_id,
    laterality, location_raw, composition, echogenicity, shape, margins,
    echogenic_foci, halo, vascularity, extrathyroidal_extension_on_us,
    chammas_type, elastography_category,
    size_cm_max,
    updated_tirads_category,
    interval_growth_flag, prior_size_mm_max,
    fna_recommended_this_nodule, fna_performed_prior_or_concurrent,
    comparison_statement,
    source_base, source_tirads_v2, source_tirads_llm, source_dynamics_llm,
    source_fna_linkage, source_us_nodules_tirads,
    is_aggregate_row, nlp_backfill_pending
)
SELECT
    research_id,
    md5(CAST(research_id AS VARCHAR) || '|' ||
        COALESCE(CAST(exam_date AS VARCHAR),'')) AS us_exam_id,
    exam_date,
    1 AS nodule_index_within_exam,
    md5(CAST(research_id AS VARCHAR) || '|' ||
        COALESCE(CAST(exam_date AS VARCHAR),'') || '|raw0') AS nodule_id,
    laterality, NULL::VARCHAR, composition, echogenicity, shape, margins,
    echogenic_foci, halo, vascularity, extrathyroidal_extension_on_us,
    chammas_type, elastography_category,
    size_cm_max,
    tirads_category,
    interval_growth_flag, prior_size_mm_max,
    fna_recommended_this_nodule, fna_performed_prior_or_concurrent,
    comparison_statement,
    FALSE, TRUE, FALSE, FALSE, FALSE, FALSE,
    FALSE, FALSE
FROM (
    SELECT * FROM _raw0_with_class
    WHERE classification = 'A_insert'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id, exam_date
        ORDER BY size_cm_max DESC NULLS LAST
    ) = 1
) ;
""")
    case_a_inserts = con.execute(f"SELECT COUNT(*) FROM {V2}").fetchone()[0] - v2_before
    log(f"  Case A inserted: {case_a_inserts:,} rows")

    # ── Case B: COALESCE UPDATE existing v2 rows from raw index=0 ──────────
    log("Case B COALESCE UPDATEs (per-field) for index=0 rows where v2 already exists")
    coalesce_fields = [
        "composition", "echogenicity", "shape", "margins", "echogenic_foci",
        "halo", "vascularity", "extrathyroidal_extension_on_us",
        "chammas_type", "elastography_category",
        "size_cm_max",
        "interval_growth_flag", "prior_size_mm_max",
        "fna_recommended_this_nodule", "fna_performed_prior_or_concurrent",
        "comparison_statement",
        "updated_tirads_category",  # raw.tirads_category
    ]
    case_b_updates_per_field: dict[str, int] = {}
    for col in coalesce_fields:
        raw_col = "tirads_category" if col == "updated_tirads_category" else col
        # COUNT how many will update
        result = con.execute(f"""
WITH src AS (
    SELECT research_id, exam_date, ANY_VALUE({raw_col}) AS val
    FROM _raw0_with_class
    WHERE classification = 'B_coalesce'
      AND {raw_col} IS NOT NULL
    GROUP BY 1, 2
)
UPDATE {V2} v
   SET {col} = COALESCE(v.{col}, src.val)
  FROM src
 WHERE v.research_id = src.research_id
   AND v.exam_date  = src.exam_date
   AND v.{col} IS NULL
   AND src.val IS NOT NULL
RETURNING v.research_id
""").fetchall()
        case_b_updates_per_field[col] = len(result)
    total_b_updates = sum(case_b_updates_per_field.values())
    log(f"  Case B field-level updates: {total_b_updates:,}  per-field={case_b_updates_per_field}")

    # ── Index=0 Conflict log: where v2 had a different non-null value ──────
    log("write Case B conflicts → us_raw_index0_conflict_v1 (per-field)")
    con.execute(f"""
CREATE OR REPLACE TABLE {CONFLICT_TABLE} (
    research_id INTEGER, exam_date DATE,
    field_name VARCHAR, raw_value VARCHAR, v2_value VARCHAR,
    detected_at TIMESTAMP
)""")
    n_conflicts = 0
    for col in coalesce_fields:
        raw_col = "tirads_category" if col == "updated_tirads_category" else col
        n = con.execute(f"""
INSERT INTO {CONFLICT_TABLE}
SELECT v.research_id, v.exam_date, ?,
       CAST(r.{raw_col} AS VARCHAR), CAST(v.{col} AS VARCHAR),
       CURRENT_TIMESTAMP
FROM (
    SELECT research_id, exam_date, ANY_VALUE({raw_col}) AS {raw_col}
    FROM _raw0_with_class
    WHERE classification = 'B_coalesce'
      AND {raw_col} IS NOT NULL
    GROUP BY 1, 2
) r
JOIN {V2} v
  ON v.research_id = r.research_id
 AND v.exam_date  = r.exam_date
WHERE v.{col} IS NOT NULL
  AND CAST(v.{col} AS VARCHAR) <> CAST(r.{raw_col} AS VARCHAR)
RETURNING 1
""", [col]).fetchall()
        n_conflicts += len(n)
    log(f"  Case B conflicts logged: {n_conflicts:,}")

    # ── Index >= 1 verification ─────────────────────────────────────────────
    log("verify index>=1 rows are already represented in v2 (per-field match)")
    con.execute(f"""
CREATE OR REPLACE TABLE {MISMATCH_TABLE} (
    research_id INTEGER, exam_date DATE, nodule_index INTEGER,
    field_name VARCHAR, raw_value VARCHAR, v2_value VARCHAR,
    detected_at TIMESTAMP
)""")
    n_mismatch = 0
    for col in ["composition", "echogenicity", "shape", "margins",
                "size_cm_max"]:
        raw_col = col
        n = con.execute(f"""
INSERT INTO {MISMATCH_TABLE}
SELECT
    r.research_id, r.exam_date, r.raw_index, ?,
    CAST(r.{raw_col} AS VARCHAR), CAST(v.{col} AS VARCHAR),
    CURRENT_TIMESTAMP
FROM _raw_typed r
LEFT JOIN {V2} v
  ON v.research_id = r.research_id
 AND v.exam_date  = r.exam_date
 AND v.nodule_index_within_exam = r.raw_index
WHERE r.raw_index >= 1
  AND r.{raw_col} IS NOT NULL
  AND (v.{col} IS NULL
       OR CAST(v.{col} AS VARCHAR) <> CAST(r.{raw_col} AS VARCHAR))
RETURNING 1
""", [col]).fetchall()
        n_mismatch += len(n)
    log(f"  index>=1 mismatches logged: {n_mismatch:,}")

    v2_after = con.execute(f"SELECT COUNT(*) FROM {V2}").fetchone()[0]
    log(f"  v2 after: {v2_after:,} rows  (Δ={v2_after - v2_before:+,d})")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "v2_before": v2_before, "v2_after": v2_after,
        "raw_total": n_raw, "raw_index0": n_idx0, "raw_indexN": n_idxN,
        "index0_classification": counts,
        "case_a_inserts": case_a_inserts,
        "case_b_updates_per_field": case_b_updates_per_field,
        "case_b_conflicts": n_conflicts,
        "indexN_mismatches": n_mismatch,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
