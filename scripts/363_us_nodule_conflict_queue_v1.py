#!/usr/bin/env python3
"""Script 363 — Build manuscript_workspace.us_nodule_conflict_queue_v1.

Phase 3 of US v2 consolidation. For every (research_id, us_exam_id,
nodule_index_within_exam) where two source tables disagree on the same field,
emit one row per conflict. Target audience: chart-review queue for manual
adjudication into canonical_us_nodule_v2 via targeted UPDATEs.

Sources joined:
  * canonical_us_nodule_characteristics_v1 (cunc) — has us_exam_id
  * canonical_us_nodule_master_v1          (cunm)
  * tirads_v2_nodules_raw                  (v2 — Qwen2.5-32B run)

Fields checked:
  composition, echogenicity, shape, margins, echogenic_foci, size_cm_max,
  tirads_reported, tirads_score_2017, tirads_category_v2, laterality.

Numeric thresholds:
  - size disagreement: >10% relative OR >0.2 cm absolute
  - tirads_score disagreement: any non-equal numeric value

review_priority:
  - 'high'   if any TIRADS field disagrees
  - 'medium' if size disagrees beyond threshold
  - 'low'    everything else
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

SCRIPT_TAG = "Script 363"
TARGET = f"{PUBLICATION_DB}.manuscript_workspace.us_nodule_conflict_queue_v1"
OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"363_us_nodule_conflict_queue_{RUN_TS}.json"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


# Fields to check. Each is (field_name, type, source_columns_dict)
# where source_columns_dict maps short tag -> sql expression on aliased table.
# Value cast to VARCHAR for the queue (universal display).

STR_FIELDS = [
    ("composition", "c.composition", "m.composition", "v.composition"),
    ("echogenicity", "c.echogenicity", "m.echogenicity", "v.echogenicity"),
    ("shape", "c.shape", "m.shape", "v.shape"),
    ("margins", "c.margins", "m.margin", "v.margin"),
    ("echogenic_foci",
     "CAST(c.echogenic_foci AS VARCHAR)",
     "m.echogenic_foci",
     "ARRAY_TO_STRING(v.echogenic_foci, '|')"),
    ("tirads_category_v2",
     "c.tirads_category_v2", "m.tirads_category_v2", "v.tirads_category"),
    ("laterality", "c.laterality", "m.laterality", "v.laterality"),
]

INT_FIELDS = [
    ("tirads_reported",
     "CAST(c.tirads_reported AS DOUBLE)",
     "NULL::DOUBLE",
     "v.tirads_reported_in_text"),
    ("tirads_score_2017",
     "c.tirads_score_2017",
     "m.tirads_score_2017",
     "v.tirads_total_points"),
]

# size: relative + absolute thresholds
SIZE_FIELD = ("size_cm_max", "c.size_cm_max", "m.size_cm", "v.size_cm_max")


def _str_conflict_arms() -> list[str]:
    arms: list[str] = []
    for field, c_expr, m_expr, v_expr in STR_FIELDS:
        priority = (
            "'high'" if field == "tirads_category_v2" else "'low'"
        )
        arms.append(f"""
SELECT
    c.research_id, c.us_exam_id, c.exam_date, c.nodule_index_within_exam,
    '{field}'                                 AS field_name,
    CAST({c_expr} AS VARCHAR)                 AS value_cunc,
    CAST({m_expr} AS VARCHAR)                 AS value_cunm,
    CAST({v_expr} AS VARCHAR)                 AS value_tirads_v2,
    CAST({c_expr} AS VARCHAR)                 AS chosen_value,
    'cunc_wins_then_cunm_then_v2'             AS precedence_rule_applied,
    {priority}                                AS review_priority
FROM cunc c
LEFT JOIN cunm m USING (research_id, exam_date, nodule_index_within_exam)
LEFT JOIN v2   v USING (research_id, exam_date, nodule_index_within_exam)
WHERE
    -- at least two distinct non-null values
    (
        SELECT COUNT(DISTINCT x) FROM (
            VALUES (NULLIF(LOWER(TRIM(CAST({c_expr} AS VARCHAR))),'')),
                   (NULLIF(LOWER(TRIM(CAST({m_expr} AS VARCHAR))),'')),
                   (NULLIF(LOWER(TRIM(CAST({v_expr} AS VARCHAR))),''))
        ) t(x) WHERE x IS NOT NULL
    ) >= 2
""")
    return arms


def _int_conflict_arms() -> list[str]:
    arms: list[str] = []
    for field, c_expr, m_expr, v_expr in INT_FIELDS:
        priority = "'high'"  # all tirads-related = high priority
        arms.append(f"""
SELECT
    c.research_id, c.us_exam_id, c.exam_date, c.nodule_index_within_exam,
    '{field}'                                 AS field_name,
    CAST({c_expr} AS VARCHAR)                 AS value_cunc,
    CAST({m_expr} AS VARCHAR)                 AS value_cunm,
    CAST({v_expr} AS VARCHAR)                 AS value_tirads_v2,
    CAST(COALESCE({c_expr}, {m_expr}, {v_expr}) AS VARCHAR)  AS chosen_value,
    'cunc_wins_then_cunm_then_v2'             AS precedence_rule_applied,
    {priority}                                AS review_priority
FROM cunc c
LEFT JOIN cunm m USING (research_id, exam_date, nodule_index_within_exam)
LEFT JOIN v2   v USING (research_id, exam_date, nodule_index_within_exam)
WHERE (
    SELECT COUNT(DISTINCT x) FROM (
        VALUES ({c_expr}), ({m_expr}), ({v_expr})
    ) t(x) WHERE x IS NOT NULL
) >= 2
""")
    return arms


def _size_conflict_arm() -> str:
    field, c_expr, m_expr, v_expr = SIZE_FIELD
    return f"""
SELECT
    c.research_id, c.us_exam_id, c.exam_date, c.nodule_index_within_exam,
    '{field}'                                 AS field_name,
    CAST({c_expr} AS VARCHAR)                 AS value_cunc,
    CAST({m_expr} AS VARCHAR)                 AS value_cunm,
    CAST({v_expr} AS VARCHAR)                 AS value_tirads_v2,
    CAST(COALESCE({c_expr}, {m_expr}, {v_expr}) AS VARCHAR)  AS chosen_value,
    'cunc_wins_then_cunm_then_v2_size'        AS precedence_rule_applied,
    'medium'                                  AS review_priority
FROM cunc c
LEFT JOIN cunm m USING (research_id, exam_date, nodule_index_within_exam)
LEFT JOIN v2   v USING (research_id, exam_date, nodule_index_within_exam)
WHERE
    -- any pair must differ by >10% relative OR >0.2 cm absolute
    (
       (ABS(COALESCE({c_expr},0) - COALESCE({m_expr},0)) > 0.2
          AND COALESCE({c_expr},0) > 0 AND COALESCE({m_expr},0) > 0
          AND ABS({c_expr} - {m_expr}) / NULLIF(GREATEST({c_expr}, {m_expr}),0) > 0.10)
    OR (ABS(COALESCE({c_expr},0) - COALESCE({v_expr},0)) > 0.2
          AND COALESCE({c_expr},0) > 0 AND COALESCE({v_expr},0) > 0
          AND ABS({c_expr} - {v_expr}) / NULLIF(GREATEST({c_expr}, {v_expr}),0) > 0.10)
    OR (ABS(COALESCE({m_expr},0) - COALESCE({v_expr},0)) > 0.2
          AND COALESCE({m_expr},0) > 0 AND COALESCE({v_expr},0) > 0
          AND ABS({m_expr} - {v_expr}) / NULLIF(GREATEST({m_expr}, {v_expr}),0) > 0.10)
    )
"""


def _build_sql() -> str:
    arms = _str_conflict_arms() + _int_conflict_arms() + [_size_conflict_arm()]
    union = " UNION ALL ".join(arms)
    return f"""
CREATE OR REPLACE TABLE {TARGET} AS
WITH cunc AS (
    SELECT research_id, us_exam_id, exam_date, nodule_index_within_exam,
           composition, echogenicity, shape, margins, echogenic_foci,
           tirads_category_v2, laterality, tirads_reported, tirads_score_2017,
           size_cm_max
    FROM {PUBLICATION_DB}.main.canonical_us_nodule_characteristics_v1
),
cunm AS (
    SELECT research_id, exam_date, nodule_index_within_exam,
           composition, echogenicity, shape, margin, echogenic_foci,
           tirads_category_v2, laterality, tirads_score_2017, size_cm
    FROM {PUBLICATION_DB}.main.canonical_us_nodule_master_v1
),
v2 AS (
    SELECT
        TRY_CAST(research_id AS INTEGER) AS research_id,
        TRY_CAST(linkage_date AS DATE)   AS exam_date,
        nodule_index_within_exam,
        ANY_VALUE(composition)           AS composition,
        ANY_VALUE(echogenicity)          AS echogenicity,
        ANY_VALUE(shape)                 AS shape,
        ANY_VALUE(margin)                AS margin,
        ANY_VALUE(echogenic_foci)        AS echogenic_foci,
        ANY_VALUE(tirads_category)       AS tirads_category,
        ANY_VALUE(laterality)            AS laterality,
        ANY_VALUE(tirads_reported_in_text) AS tirads_reported_in_text,
        ANY_VALUE(tirads_total_points)   AS tirads_total_points,
        ANY_VALUE(size_cm_max)           AS size_cm_max
    FROM {PUBLICATION_DB}.main.tirads_v2_nodules_raw
    WHERE TRY_CAST(linkage_date AS DATE) IS NOT NULL
      AND TRY_CAST(research_id AS INTEGER) IS NOT NULL
    GROUP BY 1,2,3
)
{union}
;
"""


COMMENT_SQL = (
    f"COMMENT ON TABLE {TARGET} IS "
    f"'US v2 nodule conflict queue. Grain: one row per "
    f"(research_id, us_exam_id, nodule_index_within_exam, field_name). "
    f"Built {RUN_TS} by Script 363 from cunc/cunm/tirads_v2 disagreements. "
    f"review_priority high = TIRADS, medium = size >10%/0.2cm, low = other. "
    f"Manual adjudication target — UPDATE canonical_us_nodule_v2 directly.';"
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()

    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    if not args.commit:
        log("dry-run — pass --commit to materialize conflict queue.")
        return 0

    log(f"  CREATE OR REPLACE {TARGET}")
    con.execute(_build_sql())
    con.execute(COMMENT_SQL)

    n = con.execute(f"SELECT COUNT(*) FROM {TARGET}").fetchone()[0]
    by_pri = dict(con.execute(
        f"SELECT review_priority, COUNT(*) FROM {TARGET} GROUP BY 1 ORDER BY 1"
    ).fetchall())
    by_field = dict(con.execute(
        f"SELECT field_name, COUNT(*) FROM {TARGET} GROUP BY 1 ORDER BY 2 DESC"
    ).fetchall())

    log(f"  rows={n}  by_priority={by_pri}")
    for f, c in by_field.items():
        log(f"  field={f}  rows={c}")

    if n == 0:
        raise SystemExit("Conflict queue is empty — merge logic suspect.")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS, "target": TARGET,
        "rows": n, "by_priority": by_pri, "by_field": by_field,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
