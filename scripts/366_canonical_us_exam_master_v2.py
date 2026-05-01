#!/usr/bin/env python3
"""Script 366 — Build main.canonical_us_exam_master_VIEW_v2 as a VIEW.

(Originally a CREATE TABLE builder; converted to a VIEW on 2026-04-21
because the rollup contains zero unique data — every column derives from
the 3 v2 master tables. See US_rollups_to_views_raw_schema_move_cursor_prompt
_20260421.md for the rationale and parity audit.)

Grain: one row per (research_id, us_exam_id, exam_date). Aggregates from
v2 nodule, gland, and US LN tables. Modality prefixing applied throughout:
any column referring to LN data uses the us_ln_ prefix so future
CT/PET-CT/MR/nucmed exam masters can add their own ct_ln_*, petct_ln_*,
etc. columns.

Sources:
  * canonical_us_nodule_v2        (per-nodule)
  * canonical_us_thyroid_gland_v2 (per-exam, gland)
  * canonical_us_lymph_node_v2    (per-LN, US-only)
  * canonical_us_lymph_node_events_v2 — mig_187 R-A / Logan 2026-04-30:
    distinct (research_id, exam_date) not present in structured shell;
    tagged exam_id_source='ln_nlp_only' with deterministic md5 matching
    mig_171b fallback recipe.

is_preop_exam comes from canonical_patient_master.first_surgery_date_v2
(exam_date <= surg_date) — best-effort fallback to FALSE if surg_date null.

TIRADS aggregations use acr2017_tirads_category / acr2017_tirads_points
(the post-Script-376 column names).
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
TARGET = f"{PUBLICATION_DB}.main.canonical_us_exam_master_VIEW_v2"
OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"366_us_exam_master_v2_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


def _drop_if_base_table(con, fq_name: str) -> None:
    """If a BASE TABLE exists at fq_name, drop it so CREATE OR REPLACE VIEW
    can succeed. (CREATE OR REPLACE VIEW only replaces existing VIEWs in DuckDB
    — it errors when the existing object is a table.) Safe no-op if the object
    is already a view or doesn't exist."""
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
    nodules exist, falling back to the gland or LN hash; mig_187 adds NLP-only
    exam dates from canonical_us_lymph_node_events_v2 with the mig_171b md5."""
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
CREATE OR REPLACE VIEW {TARGET} AS
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
        MAX(acr2017_tirads_category)               AS worst_tirads_category_this_exam,
        MAX(acr2017_tirads_points)                 AS worst_tirads_points_this_exam,
        MIN(acr2017_tirads_category)               AS best_tirads_category_this_exam,
        SUM(CASE WHEN UPPER(acr2017_tirads_category) = 'TR5' THEN 1 ELSE 0 END) AS count_tr5,
        SUM(CASE WHEN UPPER(acr2017_tirads_category) = 'TR4' THEN 1 ELSE 0 END) AS count_tr4,
        SUM(CASE WHEN UPPER(acr2017_tirads_category) = 'TR3' THEN 1 ELSE 0 END) AS count_tr3,
        SUM(CASE WHEN UPPER(acr2017_tirads_category) = 'TR2' THEN 1 ELSE 0 END) AS count_tr2,
        SUM(CASE WHEN UPPER(acr2017_tirads_category) = 'TR1' THEN 1 ELSE 0 END) AS count_tr1,
        BOOL_OR(nlp_backfill_pending)              AS any_nodule_pending_on_exam
    FROM {PUBLICATION_DB}.main.canonical_us_nodule_v2
    WHERE is_aggregate_row IS NOT TRUE
      AND exam_date IS NOT NULL
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
      AND exam_date IS NOT NULL
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
    WHERE exam_date IS NOT NULL
    GROUP BY 1,2
),
ln_agg AS (
    SELECT
        research_id, exam_date,
        ANY_VALUE(us_exam_id)                      AS us_exam_id_ln,
        TRUE                                       AS has_us_ln_findings,
        COUNT(*)                                   AS n_us_ln_total_on_exam,
        SUM(CASE
            WHEN suspicious_flag IS TRUE THEN 1
            WHEN suspicious_flag IS FALSE THEN 0
            WHEN COALESCE(short_axis_mm, -1e9) >= 10 THEN 1
            WHEN COALESCE(size_cm_max, -1e9) >= 1.0 THEN 1
            WHEN hilum_preserved IS FALSE THEN 1
            WHEN extranodal_extension_on_us IS TRUE THEN 1
            WHEN LOWER(TRIM(COALESCE(suspicion_level,'')))
                 IN ('suspicious','indeterminate') THEN 1
            WHEN evidence_text IS NULL THEN 0
            WHEN regexp_matches(LOWER(evidence_text),
                 '(no\\s+abnormal|unremarkable\\s+adenopath|benign.?appearing|within\\s+normal|negative\\s+for|M\\s*[0-3]\\s*adenopath|stable\\s+adenopath)') THEN 0
            ELSE 1
        END)                                       AS n_abnormal_us_ln_on_exam,
        BOOL_OR(nlp_backfill_pending)              AS any_us_ln_pending_on_exam
    FROM {PUBLICATION_DB}.main.canonical_us_lymph_node_v2
    WHERE exam_date IS NOT NULL
    GROUP BY 1,2
),
shell_exams AS (
    -- Structured US rollup spine: nodule / gland / legacy LN shell only (unchanged).
    SELECT research_id, exam_date FROM nodule_agg
    UNION
    SELECT research_id, exam_date FROM gland_agg
    UNION
    SELECT research_id, exam_date FROM ln_agg
),
ln_events_rid_date AS (
    SELECT DISTINCT
        TRIM(CAST(research_id AS VARCHAR)) AS rid_v,
        exam_date
    FROM {PUBLICATION_DB}.main.canonical_us_lymph_node_events_v2
    WHERE exam_date IS NOT NULL
      AND research_id IS NOT NULL
      AND TRIM(CAST(research_id AS VARCHAR)) <> ''
),
ln_nlp_exam_agg AS (
    -- mig_187 R-A — deterministic us_exam_id matches mig_171b fallback_ln_only md5 recipe.
    SELECT
        TRY_CAST(le.rid_v AS BIGINT) AS research_id,
        le.exam_date,
        md5('US_EXAM_V2|' || le.rid_v || '|' || CAST(le.exam_date AS VARCHAR))
            AS us_exam_id_ln_nlp_fallback
    FROM ln_events_rid_date le
    WHERE TRY_CAST(le.rid_v AS BIGINT) IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM shell_exams s
        WHERE s.research_id = TRY_CAST(le.rid_v AS BIGINT)
          AND s.exam_date IS NOT DISTINCT FROM le.exam_date
      )
),
exams AS (
    SELECT research_id, exam_date FROM shell_exams
    UNION
    SELECT research_id, exam_date FROM ln_nlp_exam_agg
),
joined AS (
    SELECT
        exams.research_id,
        COALESCE(
            n.us_exam_id_nodule,
            g.us_exam_id_gland,
            l.us_exam_id_ln,
            x.us_exam_id_ln_nlp_fallback
        ) AS us_exam_id,
        CASE WHEN x.research_id IS NOT NULL THEN 'ln_nlp_only' ELSE NULL END
            AS exam_id_source,
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
    LEFT JOIN ln_nlp_exam_agg x USING (research_id, exam_date)
    {surg_join}
)
SELECT * REPLACE (CAST(research_id AS INTEGER) AS research_id) FROM joined;
"""


COMMENT_SQL = (
    f"COMMENT ON VIEW {TARGET} IS "
    f"'US v2 per-exam master (VIEW). Grain: one row per "
    f"(research_id, us_exam_id, exam_date). Materialized by Script 366 "
    f"as a VIEW over canonical_us_nodule_v2 + canonical_us_thyroid_gland_v2 + "
    f"canonical_us_lymph_node_v2 + LN-NLP exam-date extension from "
    f"canonical_us_lymph_node_events_v2 (exam_id_source ln_nlp_only; "
    f"mig_187 R-A; mig_262 widened n_abnormal_us_ln_on_exam heuristic for "
    f"structured shells with NULL suspicious_flag; last refreshed {RUN_TS}). "
    f"LN columns are US-prefixed (has_us_ln_findings, "
    f"n_us_ln_total_on_exam, n_abnormal_us_ln_on_exam) so future "
    f"CT/PET-CT/MR/nucmed exam masters slot in as ct_ln_*, etc.';"
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

    log(f"  CREATE OR REPLACE VIEW {TARGET}")
    _drop_if_base_table(con, TARGET)
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
    n_ln_nlp = con.execute(
        f"SELECT COUNT(*) FROM {TARGET} WHERE exam_id_source = 'ln_nlp_only'"
    ).fetchone()[0]
    log(
        f"  rows={n}  pts={n_pts}  preop={n_preop}  with_ln={n_ln}  "
        f"ln_nlp_only_exams={n_ln_nlp}"
    )

    # Post–Script-389 VIEW counts are ~11.8k before / ~11.9k after mig_187 extension.
    if n < 11_000:
        raise SystemExit(f"Expected plausible US exam-master row count (>= 11k); got {n}")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS, "target": TARGET,
        "rows": n, "patients": n_pts,
        "preop_count": n_preop, "with_ln": n_ln,
        "ln_nlp_only_exams": n_ln_nlp, "surg_col": surg_col,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
