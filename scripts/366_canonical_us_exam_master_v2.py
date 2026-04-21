#!/usr/bin/env python3
"""Script 366 — Build main.canonical_us_exam_master_v2 (Phase 6a).

One row per (research_id, us_exam_id, exam_date). Aggregates from v2 nodule,
gland, and US LN tables. Modality prefixing applied throughout: any column
referring to LN data uses the us_ln_ prefix so future CT/PET-CT/MR/nucmed
exam masters can add their own ct_ln_*, petct_ln_*, etc. columns.

Sources:
  * canonical_us_nodule_v2        (per-nodule)
  * canonical_us_thyroid_gland_v2 (per-exam, gland)
  * canonical_us_lymph_node_v2    (per-LN, US-only)

is_preop_exam comes from canonical_patient_master.surg_date_canonical
(exam_date <= surg_date) — best-effort fallback to FALSE if surg_date null.
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

SCRIPT_TAG = "Script 366"
TARGET = f"{PUBLICATION_DB}.main.canonical_us_exam_master_v2"
OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"366_us_exam_master_v2_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


# Detect surgery-date column name (project may use different conventions).
# Probe shows first_surgery_date_v2 (DATE) is the canonical preop reference.
SURG_COL_PROBE_SQL = f"""
SELECT column_name FROM information_schema.columns
WHERE table_catalog = '{PUBLICATION_DB}' AND table_schema = 'main'
  AND table_name = 'canonical_patient_master'
  AND column_name IN ('first_surgery_date_v2','first_surgery_date',
                      'surg_first_date','surg_date_canonical',
                      'surgery_date','date_of_surgery')
ORDER BY
  CASE column_name
    WHEN 'first_surgery_date_v2' THEN 1
    WHEN 'first_surgery_date'    THEN 2
    WHEN 'surg_first_date'       THEN 3
    ELSE 9 END
"""


def build_sql(surg_col: str | None) -> str:
    """v2 sub-tables use independent us_exam_id hashes (cunc was hashed
    differently than my new gland/ln tables). We therefore join on
    (research_id, exam_date) and prefer the cunc-derived us_exam_id when
    nodules exist for the exam, falling back to the gland or LN hash."""
    surg_join = (
        "LEFT JOIN cpm cp ON cp.research_id = exams.research_id" if surg_col
        else ""
    )
    is_preop_expr = (
        f"CASE WHEN cp.{surg_col} IS NOT NULL "
        f"AND exams.exam_date <= cp.{surg_col} THEN TRUE ELSE FALSE END"
        if surg_col else "FALSE"
    )
    cpm_cte = (
        f"cpm AS (SELECT research_id, {surg_col} FROM "
        f"{PUBLICATION_DB}.main.canonical_patient_master),"
        if surg_col else ""
    )
    return f"""
CREATE OR REPLACE TABLE {TARGET} AS
WITH
{cpm_cte}
nodule_agg AS (
    SELECT
        research_id, exam_date,
        ANY_VALUE(us_exam_id)                      AS us_exam_id_nodule,
        COUNT(*)                                   AS n_nodules_on_exam,
        MAX(size_cm_max)                           AS largest_nodule_cm,
        BOOL_OR(LOWER(COALESCE(laterality,'')) = 'right')
            AND BOOL_OR(LOWER(COALESCE(laterality,'')) = 'left')
            AS bilateral_flag,
        BOOL_OR(LOWER(COALESCE(laterality,'')) = 'isthmus')
            OR BOOL_OR(LOWER(COALESCE(location_raw,'')) LIKE '%isthmus%')
            AS isthmus_nodule_flag,
        MAX(tirads_category_v2)                    AS worst_tirads_category_this_exam,
        MAX(tirads_score_2017)                     AS worst_tirads_points_this_exam,
        MIN(tirads_category_v2)                    AS best_tirads_category_this_exam,
        SUM(CASE WHEN UPPER(tirads_category_v2) = 'TR5' THEN 1 ELSE 0 END) AS count_tr5,
        SUM(CASE WHEN UPPER(tirads_category_v2) = 'TR4' THEN 1 ELSE 0 END) AS count_tr4,
        SUM(CASE WHEN UPPER(tirads_category_v2) = 'TR3' THEN 1 ELSE 0 END) AS count_tr3,
        SUM(CASE WHEN UPPER(tirads_category_v2) = 'TR2' THEN 1 ELSE 0 END) AS count_tr2,
        SUM(CASE WHEN UPPER(tirads_category_v2) = 'TR1' THEN 1 ELSE 0 END) AS count_tr1,
        BOOL_OR(nlp_backfill_pending)              AS any_nodule_pending_on_exam
    FROM {PUBLICATION_DB}.main.canonical_us_nodule_v2
    WHERE is_aggregate_row IS NOT TRUE
    GROUP BY 1,2
),
nodule_2nd AS (
    SELECT research_id, exam_date,
           NTH_VALUE(size_cm_max, 2) OVER (
              PARTITION BY research_id, exam_date
              ORDER BY size_cm_max DESC NULLS LAST
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
           ) AS second_largest_nodule_cm
    FROM {PUBLICATION_DB}.main.canonical_us_nodule_v2
    WHERE is_aggregate_row IS NOT TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id, exam_date
    ) = 1
),
gland_agg AS (
    SELECT research_id, exam_date,
           ANY_VALUE(us_exam_id)             AS us_exam_id_gland,
           TRUE                              AS has_gland_findings,
           BOOL_OR(nlp_backfill_pending)     AS any_gland_pending_on_exam
    FROM {PUBLICATION_DB}.main.canonical_us_thyroid_gland_v2
    GROUP BY 1,2
),
ln_agg AS (
    SELECT
        research_id, exam_date,
        ANY_VALUE(us_exam_id)                      AS us_exam_id_ln,
        TRUE                                       AS has_us_ln_findings,
        COUNT(*)                                   AS n_us_ln_total_on_exam,
        SUM(CASE WHEN suspicious_flag IS TRUE THEN 1 ELSE 0 END)
                                                   AS n_abnormal_us_ln_on_exam,
        BOOL_OR(nlp_backfill_pending)              AS any_us_ln_pending_on_exam
    FROM {PUBLICATION_DB}.main.canonical_us_lymph_node_v2
    GROUP BY 1,2
),
exams AS (
    -- universe: any (research_id, exam_date) appearing in any v2 child
    SELECT research_id, exam_date FROM nodule_agg
    UNION
    SELECT research_id, exam_date FROM gland_agg
    UNION
    SELECT research_id, exam_date FROM ln_agg
),
joined AS (
    SELECT
        exams.research_id,
        COALESCE(n.us_exam_id_nodule, g.us_exam_id_gland, l.us_exam_id_ln)
            AS us_exam_id,
        exams.exam_date,
        n.n_nodules_on_exam,
        n.largest_nodule_cm,
        n2.second_largest_nodule_cm,
        n.bilateral_flag,
        n.isthmus_nodule_flag,
        n.worst_tirads_category_this_exam,
        n.worst_tirads_points_this_exam,
        n.best_tirads_category_this_exam,
        n.count_tr5, n.count_tr4, n.count_tr3, n.count_tr2, n.count_tr1,
        COALESCE(g.has_gland_findings, FALSE)   AS has_gland_findings,
        COALESCE(l.has_us_ln_findings, FALSE)   AS has_us_ln_findings,
        l.n_us_ln_total_on_exam,
        l.n_abnormal_us_ln_on_exam,
        ROW_NUMBER() OVER (
            PARTITION BY exams.research_id ORDER BY exams.exam_date NULLS LAST
        ) AS exam_rank_for_patient,
        {is_preop_expr} AS is_preop_exam,
        (
            COALESCE(n.any_nodule_pending_on_exam, FALSE)
            OR COALESCE(g.any_gland_pending_on_exam, FALSE)
            OR COALESCE(l.any_us_ln_pending_on_exam, FALSE)
        ) AS any_nlp_backfill_pending_on_exam
    FROM exams
    LEFT JOIN nodule_agg n  USING (research_id, exam_date)
    LEFT JOIN nodule_2nd n2 USING (research_id, exam_date)
    LEFT JOIN gland_agg  g  USING (research_id, exam_date)
    LEFT JOIN ln_agg     l  USING (research_id, exam_date)
    {surg_join}
)
SELECT * FROM joined;
"""


COMMENT_SQL = (
    f"COMMENT ON TABLE {TARGET} IS "
    f"'US v2 per-exam master. Grain: one row per "
    f"(research_id, us_exam_id, exam_date). Built {RUN_TS} by Script 366 "
    f"from canonical_us_nodule_v2 + canonical_us_thyroid_gland_v2 + "
    f"canonical_us_lymph_node_v2. LN columns are US-prefixed "
    f"(has_us_ln_findings, n_us_ln_total_on_exam, n_abnormal_us_ln_on_exam) "
    f"so future CT/PET-CT/MR/nucmed exam masters slot in as ct_ln_*, etc.';"
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    surg_rows = con.execute(SURG_COL_PROBE_SQL).fetchall()
    surg_col = surg_rows[0][0] if surg_rows else None
    log(f"  surgery date column on CPM: {surg_col}")

    if not args.commit:
        log("dry-run only.")
        return 0

    log(f"  CREATE OR REPLACE {TARGET}")
    con.execute(build_sql(surg_col))
    con.execute(COMMENT_SQL)

    n = con.execute(f"SELECT COUNT(*) FROM {TARGET}").fetchone()[0]
    n_pts = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {TARGET}"
    ).fetchone()[0]
    n_preop = con.execute(
        f"SELECT COUNT(*) FROM {TARGET} WHERE is_preop_exam"
    ).fetchone()[0]
    n_ln = con.execute(
        f"SELECT COUNT(*) FROM {TARGET} WHERE has_us_ln_findings"
    ).fetchone()[0]
    log(f"  rows={n}  pts={n_pts}  preop={n_preop}  with_ln={n_ln}")

    # Sanity: should be ≥ 13,347 (existing v1 exam count)
    if n < 13_000:
        raise SystemExit(f"Expected ≥ ~13,347 exams; got {n}")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS, "target": TARGET,
        "rows": n, "patients": n_pts,
        "preop_count": n_preop, "with_ln": n_ln, "surg_col": surg_col,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
