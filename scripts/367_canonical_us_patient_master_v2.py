#!/usr/bin/env python3
"""Script 367 — Build main.canonical_us_patient_master_VIEW_v2 as a VIEW.

(Originally a CREATE TABLE builder; converted to a VIEW on 2026-04-21
because the rollup contains zero unique data — every column derives from
canonical_us_exam_master_VIEW_v2, which is itself a view over the 3 v2 master
tables. See US_rollups_to_views_raw_schema_move_cursor_prompt_20260421.md
for the rationale and parity audit.)

Grain: one row per patient. Aggregates from exam_master_v2 view.
US-prefixed LN columns so future modality patient masters don't collide.
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

SCRIPT_TAG = "Script 367"
TARGET = f"{PUBLICATION_DB}.main.canonical_us_patient_master_VIEW_v2"
OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"367_us_patient_master_v2_{RUN_TS}.json"

BUILD_SQL = f"""
CREATE OR REPLACE VIEW {TARGET} AS
WITH exam_agg AS (
    SELECT
        research_id,
        TRUE                                       AS has_any_us,
        COUNT(*)                                   AS n_us_exams,
        MIN(exam_date)                             AS first_us_date,
        MAX(exam_date)                             AS last_us_date,
        BOOL_OR(is_preop_exam)                     AS preop_us_available_flag,
        MAX(worst_tirads_category_this_exam)       AS max_tirads_category_ever,
        MAX(worst_tirads_points_this_exam)         AS max_tirads_points_ever,
        SUM(n_nodules_on_exam)                     AS n_nodules_total_across_exams,
        BOOL_OR(bilateral_flag)                    AS bilateral_disease_flag_ever,
        SUM(CASE WHEN n_nodules_on_exam >= 2 THEN 1 ELSE 0 END) > 0
                                                   AS multifocal_flag_ever,
        BOOL_OR(has_us_ln_findings)                AS has_us_ln_findings_ever,
        BOOL_OR(has_gland_findings)                AS has_gland_findings_ever,
        SUM(COALESCE(n_abnormal_us_ln_on_exam, 0)) > 0
                                                   AS any_suspicious_us_ln_ever,
        MIN(CASE WHEN n_abnormal_us_ln_on_exam IS NOT NULL
                  AND n_abnormal_us_ln_on_exam > 0
                 THEN exam_date END)               AS first_abnormal_us_ln_date,
        BOOL_OR(any_nlp_backfill_pending_on_exam)  AS any_nlp_backfill_pending_for_patient
    FROM {PUBLICATION_DB}.main.canonical_us_exam_master_VIEW_v2
    GROUP BY 1
),
nodule_first_last AS (
    -- TIRADS at first exam and at last preop exam
    SELECT
        e.research_id,
        ANY_VALUE(e.worst_tirads_category_this_exam ORDER BY e.exam_date)
            FILTER (WHERE e.exam_rank_for_patient = 1)
            AS tirads_category_at_first_exam,
        ANY_VALUE(e.worst_tirads_category_this_exam ORDER BY e.exam_date DESC)
            FILTER (WHERE e.is_preop_exam IS TRUE)
            AS tirads_category_at_last_preop_exam,
        MIN(CASE WHEN UPPER(e.worst_tirads_category_this_exam) IN ('TR4','TR5')
                 THEN e.exam_date END)
            AS first_high_risk_tirads_date
    FROM {PUBLICATION_DB}.main.canonical_us_exam_master_VIEW_v2 e
    GROUP BY 1
)
SELECT
    e.research_id,
    e.has_any_us,
    e.n_us_exams,
    e.first_us_date,
    e.last_us_date,
    e.preop_us_available_flag,
    e.max_tirads_category_ever,
    e.max_tirads_points_ever,
    nfl.tirads_category_at_first_exam,
    nfl.tirads_category_at_last_preop_exam,
    e.n_nodules_total_across_exams,
    e.bilateral_disease_flag_ever,
    e.multifocal_flag_ever,
    nfl.first_high_risk_tirads_date,
    e.has_us_ln_findings_ever,
    e.any_suspicious_us_ln_ever,
    e.first_abnormal_us_ln_date,
    e.has_gland_findings_ever,
    e.any_nlp_backfill_pending_for_patient
FROM exam_agg e
LEFT JOIN nodule_first_last nfl USING (research_id);
"""


COMMENT_SQL = (
    f"COMMENT ON VIEW {TARGET} IS "
    f"'US v2 per-patient master (VIEW). Grain: one row per research_id "
    f"(patients with any US exam). Materialized by Script 367 as a VIEW over "
    f"canonical_us_exam_master_VIEW_v2 (last refreshed {RUN_TS}). "
    f"LN columns are US-prefixed (has_us_ln_findings_ever, "
    f"any_suspicious_us_ln_ever, first_abnormal_us_ln_date) so future "
    f"modality patient masters can add ct_*, petct_*, etc. without collision.';"
)


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def _drop_if_base_table(con, fq_name: str) -> None:
    """Same helper as Script 366 — drop BASE TABLE if present so
    CREATE OR REPLACE VIEW can succeed."""
    parts = fq_name.split(".")
    if len(parts) != 3:
        return
    catalog, schema, name = parts
    row = con.execute(
        "SELECT table_type FROM information_schema.tables "
        "WHERE table_catalog = ? AND table_schema = ? AND table_name = ?",
        [catalog, schema, name],
    ).fetchone()
    if row and row[0] == "BASE TABLE":
        con.execute(f"DROP TABLE {fq_name}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()
    if not args.commit:
        log("dry-run only.")
        return 0
    _drop_if_base_table(con, TARGET)
    con.execute(BUILD_SQL)
    con.execute(COMMENT_SQL)
    n = con.execute(f"SELECT COUNT(*) FROM {TARGET}").fetchone()[0]
    n_preop = con.execute(
        f"SELECT COUNT(*) FROM {TARGET} WHERE preop_us_available_flag"
    ).fetchone()[0]
    n_ln = con.execute(
        f"SELECT COUNT(*) FROM {TARGET} WHERE has_us_ln_findings_ever"
    ).fetchone()[0]
    n_pending = con.execute(
        f"SELECT COUNT(*) FROM {TARGET} "
        f"WHERE any_nlp_backfill_pending_for_patient"
    ).fetchone()[0]
    log(f"  rows={n}  preop_pts={n_preop}  ln_pts={n_ln}  pending_pts={n_pending}")
    if n < 6_126:
        raise SystemExit(f"Expected ≥ 6,126 patients; got {n}")
    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS, "target": TARGET,
        "rows": n, "preop_pts": n_preop, "ln_pts": n_ln, "pending_pts": n_pending,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
