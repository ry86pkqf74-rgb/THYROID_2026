#!/usr/bin/env python3
"""Script 364 — Build main.canonical_us_thyroid_gland_v2 (Phase 4).

Per-exam thyroid gland (non-nodule) findings. NO LLM in this pass. Pure regex
+ COALESCE over ultrasound_reports. Parenchyma fields (Hashimoto pattern,
heterogeneity, vascularity_overall, etc.) have no current parsed source —
they remain NULL and the row carries nlp_backfill_pending = TRUE as a
diagnostic marker for the post-run reassessment.

Grain: one row per (research_id, us_exam_id, exam_date).

Sources:
  * ultrasound_reports — primary (6,793 reports / 4,074 patients)
    Lobe dimension regex: 'L x W x D cm' or 'L x W x D mm'.
  * us_nodules_tirads — fallback for patients NOT in ultrasound_reports
    (shell rows keyed on us_1_date, all measurements NULL,
     nlp_backfill_pending=TRUE).
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

SCRIPT_TAG = "Script 364"
TARGET = f"{PUBLICATION_DB}.main.canonical_us_thyroid_gland_v2"
OUT_DIR = HERE / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
DECISION_LOG = OUT_DIR / f"364_us_thyroid_gland_v2_{RUN_TS}.json"

# Regex captures: float, x/×, float, x/×, float, optional cm/mm
DIM_RE = r"([0-9]+\.?[0-9]*)\s*[x×X]\s*([0-9]+\.?[0-9]*)\s*[x×X]\s*([0-9]+\.?[0-9]*)\s*(cm|mm)?"


def log(msg: str) -> None:
    now = datetime.datetime.now(datetime.UTC).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}Z] {msg}", flush=True)


# Three-dim parser as DuckDB SQL: returns dim cm using regexp_extract_all
def _parse_lobe(prefix: str, src_col: str) -> str:
    """Return CTE column expressions parsing src_col into <prefix>_length_cm,
    <prefix>_width_cm, <prefix>_depth_cm, <prefix>_volume_ml."""
    return f"""
        TRY_CAST(regexp_extract({src_col}, '{DIM_RE}', 1) AS DOUBLE)
            / CASE WHEN LOWER(regexp_extract({src_col}, '{DIM_RE}', 4)) = 'mm'
                   THEN 10.0 ELSE 1.0 END AS {prefix}_length_cm,
        TRY_CAST(regexp_extract({src_col}, '{DIM_RE}', 2) AS DOUBLE)
            / CASE WHEN LOWER(regexp_extract({src_col}, '{DIM_RE}', 4)) = 'mm'
                   THEN 10.0 ELSE 1.0 END AS {prefix}_width_cm,
        TRY_CAST(regexp_extract({src_col}, '{DIM_RE}', 3) AS DOUBLE)
            / CASE WHEN LOWER(regexp_extract({src_col}, '{DIM_RE}', 4)) = 'mm'
                   THEN 10.0 ELSE 1.0 END AS {prefix}_depth_cm
"""


BUILD_SQL = f"""
CREATE OR REPLACE TABLE {TARGET} AS
WITH ur_parsed AS (
    SELECT
        TRY_CAST(research_id AS INTEGER) AS research_id,
        md5(research_id || '|' || COALESCE(ultrasound_date, ''))
            AS us_exam_id,
        TRY_CAST(ultrasound_date AS DATE) AS exam_date,

        {_parse_lobe('rl', 'right_lobe_dimensions')},
        {_parse_lobe('ll', 'left_lobe_dimensions')},

        TRY_CAST(regexp_extract(isthmus_thickness,
            '([0-9]+\\.?[0-9]*)\\s*(mm|cm)?', 1) AS DOUBLE)
            * CASE WHEN LOWER(regexp_extract(isthmus_thickness,
                '([0-9]+\\.?[0-9]*)\\s*(mm|cm)?', 2)) = 'cm'
                THEN 10.0 ELSE 1.0 END
            AS isthmus_thickness_mm,

        TRY_CAST(total_thyroid_volume_ml AS DOUBLE) AS total_thyroid_volume_ml,
        total_thyroid_size                          AS total_thyroid_size_text,

        clinical_impression                         AS clinical_impression_text,
        source_us_impression                        AS source_us_impression_text,
        recommendation                              AS recommendation_text,
        radiologist,
        study_indication
    FROM {PUBLICATION_DB}.main.ultrasound_reports
    WHERE TRY_CAST(research_id AS INTEGER) IS NOT NULL
),
ur_with_volumes AS (
    SELECT
        *,
        CASE
            WHEN rl_length_cm IS NOT NULL AND rl_width_cm IS NOT NULL
                 AND rl_depth_cm IS NOT NULL
            THEN PI()/6.0 * rl_length_cm * rl_width_cm * rl_depth_cm
        END AS rl_volume_ml,
        CASE
            WHEN ll_length_cm IS NOT NULL AND ll_width_cm IS NOT NULL
                 AND ll_depth_cm IS NOT NULL
            THEN PI()/6.0 * ll_length_cm * ll_width_cm * ll_depth_cm
        END AS ll_volume_ml
    FROM ur_parsed
),
ur_final AS (
    SELECT
        research_id, us_exam_id, exam_date,
        rl_length_cm, rl_width_cm, rl_depth_cm, rl_volume_ml,
        ll_length_cm, ll_width_cm, ll_depth_cm, ll_volume_ml,
        isthmus_thickness_mm,
        NULL::BOOLEAN AS pyramidal_present_flag,
        NULL::BOOLEAN AS substernal_extension_flag,
        COALESCE(total_thyroid_volume_ml,
                 COALESCE(rl_volume_ml,0) + COALESCE(ll_volume_ml,0))
            AS total_thyroid_volume_ml,
        total_thyroid_size_text,
        NULL::VARCHAR AS background_echogenicity,
        NULL::VARCHAR AS heterogeneity,
        NULL::VARCHAR AS hashimoto_pattern,
        NULL::VARCHAR AS vascularity_overall,
        NULL::VARCHAR AS calcifications_parenchymal,
        NULL::BOOLEAN AS goiter_flag,
        clinical_impression_text,
        source_us_impression_text,
        recommendation_text,
        radiologist,
        study_indication,
        TRUE  AS source_ultrasound_reports,
        FALSE AS source_us_nodules_tirads,
        TRUE  AS nlp_backfill_pending,
        CURRENT_TIMESTAMP AS extracted_at,
        '{SCRIPT_TAG}'    AS build_script
    FROM ur_with_volumes
),
ur_dedup AS (
    -- Some patient/date pairs may have multiple reports — keep the report with
    -- the most populated measurements.
    SELECT *
    FROM ur_final
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id, us_exam_id
        ORDER BY (
            (CASE WHEN rl_length_cm IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN ll_length_cm IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN isthmus_thickness_mm IS NOT NULL THEN 1 ELSE 0 END)
        ) DESC,
        LENGTH(COALESCE(clinical_impression_text,'')) DESC
    ) = 1
),
usnt_only AS (
    SELECT
        TRY_CAST(u.research_id AS INTEGER) AS research_id,
        md5(u.research_id || '|' || COALESCE(u.us_1_date,''))
            AS us_exam_id,
        TRY_CAST(u.us_1_date AS DATE) AS exam_date,
        NULL::DOUBLE AS rl_length_cm, NULL::DOUBLE AS rl_width_cm,
        NULL::DOUBLE AS rl_depth_cm,  NULL::DOUBLE AS rl_volume_ml,
        NULL::DOUBLE AS ll_length_cm, NULL::DOUBLE AS ll_width_cm,
        NULL::DOUBLE AS ll_depth_cm,  NULL::DOUBLE AS ll_volume_ml,
        NULL::DOUBLE AS isthmus_thickness_mm,
        NULL::BOOLEAN AS pyramidal_present_flag,
        NULL::BOOLEAN AS substernal_extension_flag,
        NULL::DOUBLE  AS total_thyroid_volume_ml,
        NULL::VARCHAR AS total_thyroid_size_text,
        NULL::VARCHAR AS background_echogenicity,
        NULL::VARCHAR AS heterogeneity,
        NULL::VARCHAR AS hashimoto_pattern,
        NULL::VARCHAR AS vascularity_overall,
        NULL::VARCHAR AS calcifications_parenchymal,
        NULL::BOOLEAN AS goiter_flag,
        u.us_1_impression AS clinical_impression_text,
        NULL::VARCHAR     AS source_us_impression_text,
        NULL::VARCHAR     AS recommendation_text,
        NULL::VARCHAR     AS radiologist,
        NULL::VARCHAR     AS study_indication,
        FALSE AS source_ultrasound_reports,
        TRUE  AS source_us_nodules_tirads,
        TRUE  AS nlp_backfill_pending,
        CURRENT_TIMESTAMP AS extracted_at,
        '{SCRIPT_TAG}'    AS build_script
    FROM {PUBLICATION_DB}.main.us_nodules_tirads u
    WHERE TRY_CAST(u.research_id AS INTEGER) IS NOT NULL
      AND TRY_CAST(u.research_id AS INTEGER) NOT IN
          (SELECT research_id FROM ur_dedup)
)
SELECT * FROM ur_dedup
UNION ALL
SELECT * FROM usnt_only;
"""


COMMENT_SQL = (
    f"COMMENT ON TABLE {TARGET} IS "
    f"'US gland (non-nodule) per-exam findings. Grain: one row per "
    f"(research_id, us_exam_id, exam_date). Built {RUN_TS} by Script 364 "
    f"from ultrasound_reports (regex parse of lobe dims + isthmus) plus "
    f"shell rows from us_nodules_tirads for patients without a structured "
    f"US report. Parenchyma fields (echogenicity, heterogeneity, Hashimoto, "
    f"vascularity, parenchymal calcs, goiter) have no current parsed source — "
    f"NULL with nlp_backfill_pending = TRUE on every row.';"
)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    log(f"{SCRIPT_TAG} start  commit={args.commit}")
    con = connect_locked()

    if not args.commit:
        log("dry-run only.")
        return 0

    log(f"  CREATE OR REPLACE {TARGET}")
    con.execute(BUILD_SQL)
    con.execute(COMMENT_SQL)

    n = con.execute(f"SELECT COUNT(*) FROM {TARGET}").fetchone()[0]
    n_pts = con.execute(
        f"SELECT COUNT(DISTINCT research_id) FROM {TARGET}"
    ).fetchone()[0]
    n_pending = con.execute(
        f"SELECT COUNT(*) FROM {TARGET} WHERE nlp_backfill_pending"
    ).fetchone()[0]
    n_with_dims = con.execute(
        f"""SELECT COUNT(*) FROM {TARGET}
            WHERE rl_length_cm IS NOT NULL OR ll_length_cm IS NOT NULL"""
    ).fetchone()[0]
    log(f"  rows={n}  pts={n_pts}  pending={n_pending}  with_lobe_dims={n_with_dims}")

    if n < 6_793:
        raise SystemExit(f"Expected ≥ 6,793 rows from ultrasound_reports; got {n}")

    DECISION_LOG.write_text(json.dumps({
        "script": SCRIPT_TAG, "run_ts_utc": RUN_TS, "target": TARGET,
        "rows": n, "patients": n_pts, "pending": n_pending,
        "with_lobe_dims": n_with_dims,
    }, indent=2, default=str))
    log(f"decision log: {DECISION_LOG}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
