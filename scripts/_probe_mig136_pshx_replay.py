#!/usr/bin/env python3
"""One-off mig_136 drift — replay Script 215 PSHx aggregates from MotherDuck."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))
from _md_connect import connect_locked  # noqa: E402

FIX_SQL = """
WITH
parsed AS (
  SELECT CAST(research_id AS BIGINT) AS research_id, note_row_id, note_date, note_type,
    json_extract(CAST(result_json AS JSON), '$.entities') AS entities_arr
  FROM main.note_entities_llm_past_surgical_hx
  WHERE result_json IS NOT NULL
    AND CAST(result_json AS VARCHAR) NOT LIKE '%"entities": []%'
    AND json_type(json_extract(CAST(result_json AS JSON), '$.entities')) = 'ARRAY'
    AND CAST(research_id AS BIGINT) != 11454
),
flat AS (
  SELECT research_id, note_row_id, note_date, note_type,
         UNNEST(CAST(entities_arr AS JSON[])) AS entity
  FROM parsed
),
ext AS (
  SELECT research_id, note_row_id, note_date, note_type,
    json_extract_string(entity, '$.entity_type') AS entity_type,
    json_extract_string(entity, '$.entity_date') AS entity_date,
    json_extract_string(entity, '$.present_or_negated') AS present_or_negated,
    COALESCE(TRY_CAST(json_extract(entity, '$.confidence') AS DOUBLE), 0.0) AS confidence
  FROM flat
),
pos AS (
  SELECT * FROM ext
  WHERE confidence >= 0.7
    AND (present_or_negated = 'present' OR present_or_negated IS NULL)
),
fresh AS (
  SELECT
    research_id AS rid,
    BOOL_OR(entity_type IN (
      'prior_thyroidectomy', 'prior_thyroid_surgery',
      'prior_thyroid_lobectomy', 'thyroidectomy_history')) AS r_thy,
    COUNT(CASE WHEN entity_type IN (
      'prior_thyroidectomy', 'prior_thyroid_surgery',
      'prior_thyroid_lobectomy', 'thyroidectomy_history') THEN 1 END)::BIGINT AS nm_thy,
    MIN(CASE WHEN entity_type IN (
      'prior_thyroidectomy', 'prior_thyroid_surgery',
      'prior_thyroid_lobectomy', 'thyroidectomy_history')
      THEN COALESCE(TRY_CAST(entity_date AS DATE), TRY_CAST(note_date AS DATE)) END) AS dt_thy,

    BOOL_OR(entity_type IN ('prior_fna', 'prior_biopsy', 'fna_history', 'prior_needle_biopsy')) AS r_fna,
    COUNT(CASE WHEN entity_type IN (
      'prior_fna', 'prior_biopsy', 'fna_history', 'prior_needle_biopsy') THEN 1 END)::BIGINT AS nm_fna,

    BOOL_OR(entity_type IN (
      'prior_rai', 'rai_history', 'prior_radioiodine', 'radioactive_iodine_history')) AS r_rai,
    COUNT(CASE WHEN entity_type IN (
      'prior_rai', 'rai_history', 'prior_radioiodine', 'radioactive_iodine_history')
      THEN 1 END)::BIGINT AS nm_rai,
    MIN(CASE WHEN entity_type IN (
      'prior_rai', 'rai_history', 'prior_radioiodine', 'radioactive_iodine_history')
      THEN COALESCE(TRY_CAST(entity_date AS DATE), TRY_CAST(note_date AS DATE)) END) AS dt_rai,

    BOOL_OR(entity_type IN ('prior_neck_surgery','neck_surgery_history','prior_cervical_surgery')) AS r_neck,
    COUNT(CASE WHEN entity_type IN (
      'prior_neck_surgery','neck_surgery_history','prior_cervical_surgery') THEN 1 END)::BIGINT AS nm_neck,

    BOOL_OR(entity_type IN (
      'prior_neck_dissection','prior_lymph_node_dissection','neck_dissection_history')) AS r_nd,

    BOOL_OR(entity_type IN (
      'prior_parathyroidectomy','parathyroidectomy_history','prior_parathyroid_surgery')) AS r_para,

    COUNT(DISTINCT note_row_id)::BIGINT AS src_notes,
    MIN(confidence) AS mic,
    AVG(confidence) AS mea
  FROM pos
  GROUP BY research_id
)
SELECT
  SUM(CASE WHEN COALESCE(c.pshx_nlp_prior_thyroidectomy, FALSE)
            IS DISTINCT FROM COALESCE(f.r_thy, FALSE) THEN 1 ELSE 0 END)::BIGINT AS d_thy_b,
  SUM(CASE WHEN COALESCE(c.pshx_nlp_prior_thyroidectomy_n_mentions, BIGINT '0')
            IS DISTINCT FROM COALESCE(f.nm_thy,BIGINT '0') THEN 1 ELSE 0 END)::BIGINT AS d_thy_nm,
  SUM(CASE WHEN CAST(c.pshx_nlp_prior_thyroidectomy_date AS DATE) IS DISTINCT FROM CAST(f.dt_thy AS DATE)
            AND NOT (c.pshx_nlp_prior_thyroidectomy_date IS NULL AND f.dt_thy IS NULL)
       THEN 1 ELSE 0 END)::BIGINT AS d_thy_dt,
  SUM(CASE WHEN COALESCE(c.pshx_nlp_prior_fna, FALSE) IS DISTINCT FROM COALESCE(f.r_fna,FALSE)
       THEN 1 ELSE 0 END)::BIGINT AS d_fna_b,
  SUM(CASE WHEN COALESCE(c.pshx_nlp_prior_fna_n_mentions,BIGINT '0')
            IS DISTINCT FROM COALESCE(f.nm_fna,BIGINT '0') THEN 1 ELSE 0 END)::BIGINT AS d_fna_nm,
  SUM(CASE WHEN COALESCE(c.pshx_nlp_prior_rai, FALSE) IS DISTINCT FROM COALESCE(f.r_rai,FALSE)
       THEN 1 ELSE 0 END)::BIGINT AS d_rai_b,
  SUM(CASE WHEN COALESCE(c.pshx_nlp_prior_rai_n_mentions,BIGINT '0')
            IS DISTINCT FROM COALESCE(f.nm_rai,BIGINT '0') THEN 1 ELSE 0 END)::BIGINT AS d_rai_nm,
  SUM(CASE WHEN CAST(c.pshx_nlp_prior_rai_date AS DATE) IS DISTINCT FROM CAST(f.dt_rai AS DATE)
            AND NOT (c.pshx_nlp_prior_rai_date IS NULL AND f.dt_rai IS NULL)
       THEN 1 ELSE 0 END)::BIGINT AS d_rai_dt,
  SUM(CASE WHEN COALESCE(c.pshx_nlp_prior_neck_surgery, FALSE) IS DISTINCT FROM COALESCE(f.r_neck,FALSE)
       THEN 1 ELSE 0 END)::BIGINT AS d_neck_b,
  SUM(CASE WHEN COALESCE(c.pshx_nlp_prior_neck_surgery_n_mentions,BIGINT '0')
            IS DISTINCT FROM COALESCE(f.nm_neck,BIGINT '0') THEN 1 ELSE 0 END)::BIGINT AS d_neck_nm,
  SUM(CASE WHEN COALESCE(c.pshx_nlp_prior_neck_dissection, FALSE) IS DISTINCT FROM COALESCE(f.r_nd,FALSE)
       THEN 1 ELSE 0 END)::BIGINT AS d_nd_b,
  SUM(CASE WHEN COALESCE(c.pshx_nlp_prior_parathyroidectomy, FALSE) IS DISTINCT FROM COALESCE(f.r_para,FALSE)
       THEN 1 ELSE 0 END)::BIGINT AS d_para_b,
  SUM(CASE WHEN COALESCE(c.pshx_llm_extraction_method, '') <> 'qwen3_32b'
            AND COALESCE(c.pshx_llm_extraction_method, '') <> '' THEN 1 ELSE 0 END)::BIGINT AS d_method_bad,
  SUM(CASE WHEN COALESCE(c.pshx_llm_n_source_notes, BIGINT '0')
            IS DISTINCT FROM COALESCE(f.src_notes, BIGINT '0')
            AND NOT(f.rid IS NULL AND COALESCE(c.pshx_llm_n_source_notes, BIGINT '0') = BIGINT '0')
       THEN 1 ELSE 0 END)::BIGINT AS d_src_notes,
  SUM(CASE WHEN ROUND(COALESCE(c.pshx_llm_min_confidence, -999.0), 9)
            IS DISTINCT FROM ROUND(COALESCE(f.mic,-888.0), 9)
            AND NOT(c.pshx_llm_min_confidence IS NULL AND f.mic IS NULL)
       THEN 1 ELSE 0 END)::BIGINT AS d_min_cf,
  SUM(CASE WHEN ROUND(COALESCE(c.pshx_llm_mean_confidence,-999.0), 9)
            IS DISTINCT FROM ROUND(COALESCE(f.mea,-888.0), 9)
            AND NOT(c.pshx_llm_mean_confidence IS NULL AND f.mea IS NULL)
       THEN 1 ELSE 0 END)::BIGINT AS d_mean_cf
FROM main.canonical_patient_master c
LEFT JOIN fresh f ON CAST(c.research_id AS BIGINT)=f.rid
WHERE CAST(c.research_id AS BIGINT)!=11454
"""


def main() -> None:
    con = connect_locked()
    row = con.execute(FIX_SQL).fetchone()
    cols = [d[0] for d in con.description or []]
    print(dict(zip(cols, row)))


if __name__ == "__main__":
    main()
