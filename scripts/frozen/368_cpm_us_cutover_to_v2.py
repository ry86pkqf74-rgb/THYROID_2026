#!/usr/bin/env python3
"""Script 368 — CPM cutover to US v2 (Phase 6c).

Adds parallel _v2 columns to canonical_patient_master populated from the v2
masters. v1 columns are LEFT IN PLACE so existing readers do not break;
the v1 → v2 swap is a separate follow-up after analytics validation.

Modality safety: imaging_ln_abnormal (CPM column) is modality-ambiguous
today and is NOT touched by this script. A new lnus_abnormal_any_exam_v2
column is added so the US-sourced rollup is unambiguously labeled.
Cross-modality LN rollups (e.g., ln_abnormal_any_modality_ever) wait until
the other modality canonical_<modality>_lymph_node_v2 tables exist.
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

SCRIPT_TAG = "Script 368"
CPM = f"{PUBLICATION_DB}.main.canonical_patient_master"
EXAM = f"{PUBLICATION_DB}.main.canonical_us_exam_master_v2"
PT = f"{PUBLICATION_DB}.main.canonical_us_patient_master_v2"
NOD = f"{PUBLICATION_DB}.main.canonical_us_nodule_v2"
LN = f"{PUBLICATION_DB}.main.canonical_us_lymph_node_v2"

OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"368_cpm_us_cutover_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


# ──────────────────────────────────────────────────────────────────────────
# DDL: add v2 columns idempotently
# ──────────────────────────────────────────────────────────────────────────

V2_COLUMNS: list[tuple[str, str]] = [
    ("n_us_exams_v2",                   "BIGINT"),
    ("n_us_nodules_total_v2",           "BIGINT"),
    ("dominant_nodule_size_cm_v2",      "DOUBLE"),
    ("imaging_tirads_best_v2",          "VARCHAR"),
    ("imaging_tirads_worst_v2",         "VARCHAR"),
    ("imaging_tirads_category_v2_v2",   "VARCHAR"),
    ("imaging_laterality_rollup_v2",    "VARCHAR"),
    ("max_tirads_ever_v2",              "DOUBLE"),
    ("preop_tirads_best_v2",            "VARCHAR"),
    ("preop_tirads_category_v2",        "VARCHAR"),
    ("lnus_abnormal_any_exam_v2",       "BOOLEAN"),
    ("lnus_n_us_with_ln_assessment_v2", "BIGINT"),
    ("lnus_first_abnormal_us_ln_date_v2", "DATE"),
    ("us_v2_any_nlp_backfill_pending",  "BOOLEAN"),
]


def add_v2_columns(con) -> list[str]:
    existing = {
        r[0] for r in con.execute(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_catalog='{PUBLICATION_DB}' "
            f"AND table_schema='main' AND table_name='canonical_patient_master'"
        ).fetchall()
    }
    added: list[str] = []
    for col, dtype in V2_COLUMNS:
        if col in existing:
            continue
        con.execute(f"ALTER TABLE {CPM} ADD COLUMN {col} {dtype}")
        added.append(col)
    return added


# ──────────────────────────────────────────────────────────────────────────
# Population: build a per-patient stage table, then UPDATE CPM from it
# ──────────────────────────────────────────────────────────────────────────

POP_SQL = f"""
CREATE OR REPLACE TEMP TABLE _us_v2_cpm_stage AS
WITH dominant AS (
    SELECT research_id,
           MAX(largest_nodule_cm) AS dominant_nodule_size_cm_v2
    FROM {EXAM}
    GROUP BY 1
),
laterality_rollup AS (
    SELECT research_id,
           CASE
             WHEN BOOL_OR(LOWER(COALESCE(laterality,'')) = 'right')
              AND BOOL_OR(LOWER(COALESCE(laterality,'')) = 'left')
                  THEN 'bilateral'
             WHEN BOOL_OR(LOWER(COALESCE(laterality,'')) = 'right')
                  THEN 'right'
             WHEN BOOL_OR(LOWER(COALESCE(laterality,'')) = 'left')
                  THEN 'left'
             WHEN BOOL_OR(LOWER(COALESCE(laterality,'')) = 'isthmus')
                  THEN 'isthmus_only'
             ELSE NULL
           END AS imaging_laterality_rollup_v2
    FROM {NOD}
    WHERE is_aggregate_row IS NOT TRUE
    GROUP BY 1
),
preop AS (
    SELECT research_id,
           MIN(worst_tirads_category_this_exam) AS preop_tirads_best_v2,
           MAX(worst_tirads_category_this_exam) AS preop_tirads_category_v2
    FROM {EXAM}
    WHERE is_preop_exam IS TRUE
    GROUP BY 1
),
ln_rollup AS (
    SELECT research_id,
           BOOL_OR(suspicious_flag IS TRUE) AS lnus_abnormal_any_exam_v2,
           COUNT(DISTINCT us_exam_id)       AS lnus_n_us_with_ln_assessment_v2
    FROM {LN}
    GROUP BY 1
)
SELECT
    p.research_id,
    p.n_us_exams                  AS n_us_exams_v2,
    p.n_nodules_total_across_exams AS n_us_nodules_total_v2,
    d.dominant_nodule_size_cm_v2,
    p.max_tirads_category_ever    AS imaging_tirads_worst_v2,
    NULL::VARCHAR                 AS imaging_tirads_best_v2_stub,  -- best per patient
    p.max_tirads_category_ever    AS imaging_tirads_category_v2_v2,
    lr.imaging_laterality_rollup_v2,
    p.max_tirads_points_ever      AS max_tirads_ever_v2,
    pre.preop_tirads_best_v2,
    pre.preop_tirads_category_v2,
    COALESCE(ln.lnus_abnormal_any_exam_v2, FALSE)
                                  AS lnus_abnormal_any_exam_v2,
    COALESCE(ln.lnus_n_us_with_ln_assessment_v2, 0)
                                  AS lnus_n_us_with_ln_assessment_v2,
    p.first_abnormal_us_ln_date   AS lnus_first_abnormal_us_ln_date_v2,
    p.any_nlp_backfill_pending_for_patient
                                  AS us_v2_any_nlp_backfill_pending
FROM {PT} p
LEFT JOIN dominant         d  USING (research_id)
LEFT JOIN laterality_rollup lr USING (research_id)
LEFT JOIN preop            pre USING (research_id)
LEFT JOIN ln_rollup        ln  USING (research_id);
"""


# Best (= lowest) TIRADS per patient — separate query because v1 column name
# imaging_tirads_best maps to most-favorable, which is MIN over per-exam best.
BEST_TIRADS_SQL = f"""
UPDATE _us_v2_cpm_stage SET imaging_tirads_best_v2_stub = sub.best_v2
FROM (
    SELECT research_id, MIN(best_tirads_category_this_exam) AS best_v2
    FROM {EXAM}
    GROUP BY 1
) sub
WHERE _us_v2_cpm_stage.research_id = sub.research_id;
"""


def update_cpm_sql() -> str:
    set_pairs = []
    for col, _ in V2_COLUMNS:
        if col == "imaging_tirads_best_v2":
            set_pairs.append(f"{col} = s.imaging_tirads_best_v2_stub")
        else:
            set_pairs.append(f"{col} = s.{col}")
    set_clause = ",\n  ".join(set_pairs)
    return f"""
UPDATE {CPM} t
   SET
  {set_clause}
  FROM _us_v2_cpm_stage s
 WHERE t.research_id = s.research_id;
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    if not args.commit:
        log("dry-run only.")
        return 0

    log("  ALTER TABLE canonical_patient_master ADD COLUMN ... (idempotent)")
    added = add_v2_columns(con)
    log(f"  added columns: {added if added else '(none — already present)'}")

    log("  build temp stage table _us_v2_cpm_stage")
    con.execute(POP_SQL)
    con.execute(BEST_TIRADS_SQL)

    log("  UPDATE canonical_patient_master from stage")
    con.execute(update_cpm_sql())

    n_updated = con.execute(
        f"SELECT COUNT(*) FROM {CPM} WHERE n_us_exams_v2 IS NOT NULL"
    ).fetchone()[0]
    n_total = con.execute(f"SELECT COUNT(*) FROM {CPM}").fetchone()[0]
    n_ln_v2 = con.execute(
        f"SELECT COUNT(*) FROM {CPM} WHERE lnus_abnormal_any_exam_v2 IS TRUE"
    ).fetchone()[0]
    log(f"  CPM rows updated with v2 cols: {n_updated} of {n_total} "
        f"  lnus_abnormal_v2_TRUE={n_ln_v2}")

    if n_total != 10_871:
        raise SystemExit(
            f"CPM row count drifted: {n_total} (expected 10,871)"
        )
    if n_updated < 6_126:
        raise SystemExit(
            f"Too few CPM rows have v2 US data: {n_updated} (expected ≥ 6,126)"
        )

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS,
        "added_columns": added,
        "cpm_total_rows": n_total,
        "cpm_rows_with_v2_us": n_updated,
        "lnus_abnormal_v2_true": n_ln_v2,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
