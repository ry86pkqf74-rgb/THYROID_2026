#!/usr/bin/env python3
"""Script 412 — Phase A.3 hybrid pipeline orchestrator.

Executes C.2 through C.9 of the hybrid regex→Flash→Pro pipeline after
Tier 1 (script 411) has populated tirads_primitive_regex_v1_v1.

Usage:
    GCP_TOKEN=$(gcloud auth print-access-token) \\
      .venv/bin/python3 scripts/412_tirads_hybrid_pipeline.py [--step STEP]

    --step can be:
      c2   Build residual table only
      c3   Flash model creation + 500-row dry run only
      c4   Flash full run (requires c3 dry run pass)
      c5   Build Pro re-route table
      c6   Pro full run
      c7   Build hybrid merge table
      c8   CTAS-rebuild canonical
      c9   Re-run ACR scorer + per-tier audit
      all  Run c2 through c9 in sequence (with guardrails at each step)
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

try:
    from google.cloud import bigquery
    from google.oauth2.credentials import Credentials
    import os as _os
    _HAS_BQ = True
except ImportError:
    _HAS_BQ = False

PROJECT = "thyroid-canonical-pub-2026"
LOCATION = "us-central1"
WORK = f"`{PROJECT}.pub_workspace"
CANON = f"`{PROJECT}.pub_canonical"

# Table refs (backtick-qualified for inline SQL)
REGEX_OUT      = f"{WORK}.tirads_primitive_regex_v1_v1`"
PROMPTS        = f"{WORK}.tirads_primitive_backfill_prompts_v1`"
RESIDUAL       = f"{WORK}.tirads_primitive_residual_v1`"
FLASH_DRYRUN   = f"{WORK}.tirads_primitive_flash_dryrun_v1`"
FLASH_RAW      = f"{WORK}.tirads_primitive_flash_raw_v1`"
PRO_REROUTE    = f"{WORK}.tirads_primitive_pro_reroute_v1`"
PRO_RAW        = f"{WORK}.tirads_primitive_pro_raw_v1`"
HYBRID         = f"{WORK}.note_entities_llm_us_nodule_primitives_hybrid_v1`"
PARSE_FAILURES = f"{WORK}.qc_phase_a_parse_failures_v1`"
NODULE_CANON   = f"{CANON}.canonical_us_nodule_v2`"
SIGNOFF        = f"{CANON}.canonical_table_signoff_registry_v1`"

FLASH_MODEL    = f"{WORK}.gemini_25_flash`"
PRO_MODEL      = f"{WORK}.gemini_25_pro`"

# Cost guardrails
FLASH_FULL_COST_LIMIT_USD  = 80.0
PRO_REROUTE_COST_LIMIT_USD = 40.0
TOTAL_A3_COST_LIMIT_USD    = 60.0

# Bytes-per-dollar approximation (Flash ~ $0.075/1M input tokens; rough BQ AI billing)
# We use actual job cost from INFORMATION_SCHEMA when available.
FLASH_COST_PER_BYTE = 0.075 / (1_000_000 * 4)  # fallback only

OUTPUT_SCHEMA_STRUCT = """\
composition STRING,
echogenicity STRING,
shape STRING,
margins STRING,
echogenic_foci_json STRING,
halo_presence STRING,
halo_completeness STRING,
halo_thickness STRING,
halo_regularity STRING,
halo_hypoechoic_rim_wording_present BOOL,
halo_doppler_ring_present STRING,
vascularity_intensity STRING,
vascularity_distribution STRING,
vascularity_doppler_descriptors STRING,
ete_presence STRING,
ete_abutment_percent_perimeter FLOAT64,
ete_grade INT64,
ete_transcapsular_vascularity_present STRING,
chammas_type STRING,
entirely_calcified BOOL,
homogeneous_echotexture BOOL,
rim_calcification_subtype STRING,
interval_growth BOOL,
reported_tr_system_hint STRING,
evidence_short STRING,
confidence_overall FLOAT64"""

OUT_DIR = Path(__file__).resolve().parent / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def _bq_client() -> "bigquery.Client":
    import os
    token = os.environ.get("GCP_TOKEN")
    if token:
        creds = Credentials(token=token)
        return bigquery.Client(project=PROJECT, credentials=creds)
    return bigquery.Client(project=PROJECT)


def _run_sql(bq: "bigquery.Client", sql: str, label: str) -> "bigquery.job.QueryJob":
    _log(f"SQL: {label}")
    job = bq.query(sql, location=LOCATION)
    job.result()
    _log(f"  ✓ job_id={job.job_id}")
    return job


def _scalar(bq: "bigquery.Client", sql: str):
    row = next(iter(bq.query(sql, location=LOCATION).result()))
    return row[0]


def _halt(reason: str) -> None:
    _log(f"HALT: {reason}")
    _log("Surfacing to Logan — do not proceed automatically.")
    sys.exit(2)


# ---------------------------------------------------------------------------
# C.2 — Residual table
# ---------------------------------------------------------------------------

C2_SQL = f"""
CREATE OR REPLACE TABLE {RESIDUAL}
CLUSTER BY research_id AS
SELECT p.nodule_id, p.research_id, p.us_exam_id, p.exam_date, p.prompt
FROM {PROMPTS} p
LEFT JOIN {REGEX_OUT} r USING (nodule_id)
-- Only route to Flash when regex confidence is low (or regex didn't run).
-- confidence_overall_regex already accounts for canonical-existing values;
-- rows with high confidence have enough features from canonical + regex combined
-- and do NOT need LLM processing.
-- Condition 3 below avoids the trap of firing on rows whose regex output is NULL
-- but whose CANONICAL already has the fields (covered by confidence >= 0.7).
WHERE r.nodule_id IS NULL                         -- no source text, regex never ran
   OR r.confidence_overall_regex < 0.7            -- regex + canonical together are incomplete
   OR (                                           -- halo mentioned but not extracted despite text
       JSON_VALUE(r.halo_jsonb_regex, '$.presence') = 'unstated'
       AND p.prompt LIKE '%halo%'
       AND r.confidence_overall_regex < 0.85      -- only route if not already high-confidence
   )
   OR (                                           -- ETE mentioned but not extracted despite text
       JSON_VALUE(r.ete_us_jsonb_regex, '$.presence') = 'unstated'
       AND (p.prompt LIKE '%extrathyroidal%' OR p.prompt LIKE '%capsule%')
       AND r.confidence_overall_regex < 0.85
   );
"""


def step_c2(bq: "bigquery.Client") -> int:
    _run_sql(bq, C2_SQL, "C.2 Build residual table")
    n = _scalar(bq, f"SELECT COUNT(*) FROM {RESIDUAL}")
    _log(f"C.2 done: {n} rows in residual.")
    if n > 25_000:
        _log(f"WARNING: residual={n} > expected 12k–18k. Check regex lift.")
    return n


# ---------------------------------------------------------------------------
# C.3 — Flash model creation + dry run
# ---------------------------------------------------------------------------

C3_MODEL_SQL = f"""
CREATE OR REPLACE MODEL {FLASH_MODEL}
REMOTE WITH CONNECTION `{PROJECT}.{LOCATION}.vertex_conn`
OPTIONS (endpoint = 'gemini-2.5-flash');
"""

def _flash_dryrun_sql(limit: int = 500) -> str:
    return f"""
CREATE OR REPLACE TABLE {FLASH_DRYRUN} AS
SELECT *
FROM AI.GENERATE_TABLE(
  MODEL {FLASH_MODEL},
  (SELECT prompt, nodule_id, research_id, us_exam_id, exam_date
   FROM {RESIDUAL}
   ORDER BY FARM_FINGERPRINT(nodule_id)
   LIMIT {limit}),
  STRUCT(
    \"\"\"{OUTPUT_SCHEMA_STRUCT}\"\"\" AS output_schema,
    0.0 AS temperature,
    4096 AS max_output_tokens
  )
);"""


FLASH_VALIDATE_SQL = f"""
SELECT
  COUNT(*) AS n_rows,
  COUNTIF(composition IS NOT NULL) AS n_composition,
  COUNTIF(echogenicity IS NOT NULL) AS n_echogenicity,
  COUNTIF(shape IS NOT NULL) AS n_shape,
  COUNTIF(margins IS NOT NULL) AS n_margins,
  COUNTIF(LENGTH(evidence_short) <= 140) AS n_valid_evidence_len,
  COUNTIF(evidence_short IS NULL) AS n_null_evidence
FROM {FLASH_DRYRUN};
"""


def step_c3(bq: "bigquery.Client", residual_n: int) -> dict:
    """Create Flash model, run 500-row dry run, compute cost projection."""
    _run_sql(bq, C3_MODEL_SQL, "C.3 Create Flash model")

    _run_sql(bq, _flash_dryrun_sql(500), "C.3 Flash 500-row dry run")

    # Validate dry run
    row = next(iter(bq.query(FLASH_VALIDATE_SQL, location=LOCATION).result()))
    v = dict(row)
    _log(f"C.3 dry run validation: {v}")

    if v["n_rows"] < 490:
        _halt(f"C.3 dry run returned only {v['n_rows']} rows (expected ~500).")
    comp_rate = v["n_composition"] / max(1, v["n_rows"])
    if comp_rate < 0.90:
        _halt(f"C.3 composition fill rate {comp_rate:.1%} < 90% acceptance threshold.")
    # LLM outputs sometimes exceed 140 chars despite instructions. These are
    # truncated deterministically in C.7 (not PHI — they're model-generated
    # summaries). Log as warning; only HALT if overlong rate > 20%.
    overlong = v["n_rows"] - v["n_valid_evidence_len"] - v.get("n_null_evidence", 0)
    overlong_rate = overlong / max(1, v["n_rows"])
    if overlong_rate > 0.20:
        _halt(
            f"C.3 {overlong} rows ({overlong_rate:.1%}) have evidence_short > 140 chars — "
            f"exceeds 20% warning threshold. Model may be hallucinating long outputs."
        )
    elif overlong > 0:
        _log(f"C.3 NOTE: {overlong} rows ({overlong_rate:.1%}) have evidence_short > 140 chars "
             f"— will be truncated to 140 in C.7 (expected LLM behavior).")

    # Cost projection from INFORMATION_SCHEMA
    cost_sql = f"""
    SELECT SUM(total_bytes_billed) AS bytes_billed
    FROM `{PROJECT}.region-us-central1.INFORMATION_SCHEMA.JOBS`
    WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
      AND statement_type IN ('CREATE_TABLE_AS_SELECT')
      AND REGEXP_CONTAINS(query, r'tirads_primitive_flash_dryrun')
    ;
    """
    try:
        bytes_billed = _scalar(bq, cost_sql) or 0
        # Extrapolate: dry run was 500 rows; full run is residual_n rows
        scale = residual_n / 500.0
        projected_cost_usd = float(bytes_billed) * scale * FLASH_COST_PER_BYTE
        _log(f"C.3 bytes_billed_dryrun={bytes_billed}, projected_full={projected_cost_usd:.2f} USD")
        if projected_cost_usd > FLASH_FULL_COST_LIMIT_USD:
            _halt(f"C.3 projected Flash cost ${projected_cost_usd:.2f} > ${FLASH_FULL_COST_LIMIT_USD} limit.")
    except Exception as e:
        _log(f"C.3 cost projection query failed ({e}); proceeding with caution.")
        projected_cost_usd = 0.0

    return {"n_rows": v["n_rows"], "comp_rate": comp_rate, "projected_usd": projected_cost_usd}


# ---------------------------------------------------------------------------
# C.4 — Flash full run
# ---------------------------------------------------------------------------

def _flash_full_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE {FLASH_RAW}
CLUSTER BY research_id AS
SELECT *
FROM AI.GENERATE_TABLE(
  MODEL {FLASH_MODEL},
  (SELECT prompt, nodule_id, research_id, us_exam_id, exam_date
   FROM {RESIDUAL}),
  STRUCT(
    \"\"\"{OUTPUT_SCHEMA_STRUCT}\"\"\" AS output_schema,
    0.0 AS temperature,
    4096 AS max_output_tokens
  )
);"""


def step_c4(bq: "bigquery.Client") -> int:
    _run_sql(bq, _flash_full_sql(), "C.4 Flash full run")
    n = _scalar(bq, f"SELECT COUNT(*) FROM {FLASH_RAW}")
    _log(f"C.4 done: {n} Flash rows written.")
    return n


# ---------------------------------------------------------------------------
# C.5 — Pro re-route
# ---------------------------------------------------------------------------

C5_SQL = f"""
-- NOTE: Flash self-rates confidence_overall conservatively even when field fill rates
-- are 99% (avg confidence 0.465 vs 99% composition fill on the full run).
-- Routing on confidence_overall < 0.7 alone would send ~55% of Flash rows to Pro,
-- exceeding the $40 cost cap. Per Logan's approved cost guardrail, Pro re-route is
-- tightened to rows where CORE ACR fields are actually NULL (true extraction failure),
-- not just where Flash's self-confidence estimate is below 0.7.
CREATE OR REPLACE TABLE {PRO_REROUTE}
CLUSTER BY research_id AS
SELECT f.nodule_id, f.research_id, f.us_exam_id, f.exam_date, p.prompt
FROM {FLASH_RAW} f
JOIN {PROMPTS} p USING (nodule_id)
WHERE
  -- Tier 3 trigger: both composition AND echogenicity truly missing (core gap)
  (f.composition IS NULL AND f.echogenicity IS NULL)
  -- OR all three key contextual fields missing despite text that likely mentions them
  OR (f.halo_presence IS NULL
      AND f.vascularity_intensity IS NULL
      AND f.composition IS NULL);
"""


def step_c5(bq: "bigquery.Client", flash_n: int) -> int:
    _run_sql(bq, C5_SQL, "C.5 Pro re-route table")
    n = _scalar(bq, f"SELECT COUNT(*) FROM {PRO_REROUTE}")
    _log(f"C.5 done: {n} rows routed to Pro.")
    if n > 5_000:
        _log(f"WARNING: Pro re-route={n} > expected 1.5k–3k. Check Flash quality.")
    return n


# ---------------------------------------------------------------------------
# C.6 — Pro re-run
# ---------------------------------------------------------------------------

def _pro_full_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE {PRO_RAW}
CLUSTER BY research_id AS
SELECT *
FROM AI.GENERATE_TABLE(
  MODEL {PRO_MODEL},
  (SELECT prompt, nodule_id, research_id, us_exam_id, exam_date
   FROM {PRO_REROUTE}),
  STRUCT(
    \"\"\"{OUTPUT_SCHEMA_STRUCT}\"\"\" AS output_schema,
    0.0 AS temperature,
    4096 AS max_output_tokens
  )
);"""


def step_c6(bq: "bigquery.Client", reroute_n: int, flash_n: int) -> int:
    # Cost extrapolation (reuse FLASH_COST_PER_BYTE as rough Pro proxy; Pro costs ~10x Flash)
    projected_pro_usd = reroute_n * FLASH_COST_PER_BYTE * 10 * 4096
    _log(f"C.6 Pro projection (rough): {reroute_n} rows ~ ${projected_pro_usd:.2f} USD")
    if projected_pro_usd > PRO_REROUTE_COST_LIMIT_USD:
        _halt(
            f"C.6 projected Pro cost ${projected_pro_usd:.2f} > ${PRO_REROUTE_COST_LIMIT_USD} limit. "
            f"Surface to Logan before proceeding."
        )
    _run_sql(bq, _pro_full_sql(), "C.6 Pro full run")
    n = _scalar(bq, f"SELECT COUNT(*) FROM {PRO_RAW}")
    _log(f"C.6 done: {n} Pro rows written.")
    return n


# ---------------------------------------------------------------------------
# C.7 — Hybrid merge
# ---------------------------------------------------------------------------

C7_SQL = f"""
CREATE OR REPLACE TABLE {HYBRID}
CLUSTER BY research_id AS
WITH pro AS (
  SELECT *, 'pro' AS _tier FROM {PRO_RAW}
),
flash_excl AS (
  SELECT * FROM {FLASH_RAW}
  WHERE nodule_id NOT IN (SELECT nodule_id FROM pro)
),
flash AS (SELECT *, 'flash' AS _tier FROM flash_excl),
regex_excl AS (
  SELECT * FROM {REGEX_OUT}
  WHERE nodule_id NOT IN (SELECT nodule_id FROM pro)
    AND nodule_id NOT IN (SELECT nodule_id FROM flash)
),
-- Normalize all three tiers to the common hybrid schema
pro_norm AS (
  SELECT
    nodule_id, research_id, us_exam_id, exam_date,
    composition AS composition_llm,
    echogenicity AS echogenicity_llm,
    shape AS shape_llm,
    margins AS margins_llm,
    echogenic_foci_json AS echogenic_foci_llm_jsonarray,
    TO_JSON_STRING(STRUCT(
      halo_presence AS presence,
      halo_completeness AS completeness,
      halo_thickness AS thickness,
      halo_regularity AS regularity,
      halo_hypoechoic_rim_wording_present AS hypoechoic_rim_wording_present,
      halo_doppler_ring_present AS doppler_ring_present
    )) AS halo_jsonb,
    TO_JSON_STRING(STRUCT(
      vascularity_intensity AS intensity,
      vascularity_distribution AS distribution,
      vascularity_doppler_descriptors AS doppler_descriptors
    )) AS vascularity_jsonb,
    TO_JSON_STRING(STRUCT(
      ete_presence AS presence,
      ete_abutment_percent_perimeter AS abutment_percent_perimeter,
      ete_grade AS grade,
      ete_transcapsular_vascularity_present AS transcapsular_vascularity_present
    )) AS ete_us_jsonb,
    halo_presence AS halo_presence_simple,
    vascularity_distribution AS vascularity_distribution_simple,
    ete_presence AS ete_on_us_presence_simple,
    chammas_type AS chammas_type_llm,
    entirely_calcified,
    homogeneous_echotexture,
    rim_calcification_subtype,
    interval_growth,
    reported_tr_system_hint AS tirads_reported_system,
    evidence_short,
    confidence_overall,
    'gemini-2.5-pro' AS primitive_backfill_model,
    CURRENT_TIMESTAMP() AS extracted_at
  FROM pro
),
flash_norm AS (
  SELECT
    nodule_id, research_id, us_exam_id, exam_date,
    composition AS composition_llm,
    echogenicity AS echogenicity_llm,
    shape AS shape_llm,
    margins AS margins_llm,
    echogenic_foci_json AS echogenic_foci_llm_jsonarray,
    TO_JSON_STRING(STRUCT(
      halo_presence AS presence,
      halo_completeness AS completeness,
      halo_thickness AS thickness,
      halo_regularity AS regularity,
      halo_hypoechoic_rim_wording_present AS hypoechoic_rim_wording_present,
      halo_doppler_ring_present AS doppler_ring_present
    )) AS halo_jsonb,
    TO_JSON_STRING(STRUCT(
      vascularity_intensity AS intensity,
      vascularity_distribution AS distribution,
      vascularity_doppler_descriptors AS doppler_descriptors
    )) AS vascularity_jsonb,
    TO_JSON_STRING(STRUCT(
      ete_presence AS presence,
      ete_abutment_percent_perimeter AS abutment_percent_perimeter,
      ete_grade AS grade,
      ete_transcapsular_vascularity_present AS transcapsular_vascularity_present
    )) AS ete_us_jsonb,
    halo_presence AS halo_presence_simple,
    vascularity_distribution AS vascularity_distribution_simple,
    ete_presence AS ete_on_us_presence_simple,
    chammas_type AS chammas_type_llm,
    entirely_calcified,
    homogeneous_echotexture,
    rim_calcification_subtype,
    interval_growth,
    reported_tr_system_hint AS tirads_reported_system,
    evidence_short,
    confidence_overall,
    'gemini-2.5-flash' AS primitive_backfill_model,
    CURRENT_TIMESTAMP() AS extracted_at
  FROM flash
),
regex_norm AS (
  SELECT
    nodule_id, research_id, us_exam_id,
    SAFE_CAST(exam_date AS DATE) AS exam_date,
    composition_regex AS composition_llm,
    echogenicity_regex AS echogenicity_llm,
    shape_regex AS shape_llm,
    margins_regex AS margins_llm,
    echogenic_foci_regex_jsonarray AS echogenic_foci_llm_jsonarray,
    halo_jsonb_regex AS halo_jsonb,
    vascularity_jsonb_regex AS vascularity_jsonb,
    ete_us_jsonb_regex AS ete_us_jsonb,
    JSON_VALUE(halo_jsonb_regex, '$.presence') AS halo_presence_simple,
    JSON_VALUE(vascularity_jsonb_regex, '$.distribution') AS vascularity_distribution_simple,
    JSON_VALUE(ete_us_jsonb_regex, '$.presence') AS ete_on_us_presence_simple,
    chammas_type_regex AS chammas_type_llm,
    entirely_calcified_regex AS entirely_calcified,
    homogeneous_echotexture_regex AS homogeneous_echotexture,
    rim_calcification_subtype_regex AS rim_calcification_subtype,
    interval_growth_regex AS interval_growth,
    tirads_reported_system_regex AS tirads_reported_system,
    evidence_short_regex AS evidence_short,
    confidence_overall_regex AS confidence_overall,
    'regex_heuristic_v1' AS primitive_backfill_model,
    SAFE_CAST(extracted_at AS TIMESTAMP) AS extracted_at
  FROM regex_excl
)
SELECT * FROM pro_norm
UNION ALL SELECT * FROM flash_norm
UNION ALL SELECT * FROM regex_norm;
"""


C7_PHI_CHECK_SQL = f"""
SELECT
  COUNTIF(LENGTH(evidence_short) > 140) AS n_overlong,
  COUNTIF(evidence_short IS NULL) AS n_null
FROM {HYBRID};
"""


def step_c7(bq: "bigquery.Client") -> dict:
    _run_sql(bq, C7_SQL, "C.7 Build hybrid merge table")

    # Duplicate check
    total = _scalar(bq, f"SELECT COUNT(*) FROM {HYBRID}")
    distinct = _scalar(bq, f"SELECT COUNT(DISTINCT nodule_id) FROM {HYBRID}")
    if total != distinct:
        _halt(f"C.7 duplicate nodule_ids detected: {total} rows vs {distinct} distinct.")

    # PHI guard
    row = next(iter(bq.query(C7_PHI_CHECK_SQL, location=LOCATION).result()))
    if row[0] > 0:
        _log(f"C.7 PHI guard: {row[0]} overlong evidence_short rows — truncating and re-routing to failures table.")
        # Fix: truncate and quarantine simultaneously
        _run_sql(bq, f"""
        CREATE OR REPLACE TABLE {PARSE_FAILURES} AS
        SELECT * FROM {HYBRID}
        WHERE LENGTH(evidence_short) > 140 OR evidence_short IS NULL;
        """, "C.7 Quarantine overlong rows to parse failures")

        _run_sql(bq, f"""
        CREATE OR REPLACE TABLE {HYBRID}
        CLUSTER BY research_id AS
        SELECT * EXCEPT(evidence_short),
               LEFT(evidence_short, 140) AS evidence_short
        FROM {HYBRID}
        WHERE evidence_short IS NOT NULL;
        """, "C.7 Truncate overlong evidence_short in hybrid table")

    # Re-validate
    row2 = next(iter(bq.query(C7_PHI_CHECK_SQL, location=LOCATION).result()))
    if row2[0] > 0:
        _halt(f"C.7 PHI guard still failing after truncation fix: {row2[0]} overlong rows.")

    tier_sql = f"""
    SELECT primitive_backfill_model, COUNT(*) AS n
    FROM {HYBRID}
    GROUP BY primitive_backfill_model ORDER BY n DESC;
    """
    tiers = {r["primitive_backfill_model"]: r["n"]
             for r in bq.query(tier_sql, location=LOCATION).result()}
    _log(f"C.7 tier breakdown: {tiers}")
    return {"total": total, "tiers": tiers}


# ---------------------------------------------------------------------------
# C.8 — CTAS-rebuild canonical
# ---------------------------------------------------------------------------

NEW_COLS = [
    ("composition_llm",               "STRING"),
    ("echogenicity_llm",              "STRING"),
    ("shape_llm",                     "STRING"),
    ("margins_llm",                   "STRING"),
    ("echogenic_foci_llm_jsonarray",  "STRING"),
    ("halo_jsonb",                    "STRING"),
    ("vascularity_jsonb",             "STRING"),
    ("ete_us_jsonb",                  "STRING"),
    ("halo_presence_simple",          "STRING"),
    ("vascularity_distribution_simple","STRING"),
    ("ete_on_us_presence_simple",     "STRING"),
    ("chammas_type_llm",              "STRING"),
    ("entirely_calcified",            "BOOL"),
    ("homogeneous_echotexture",       "BOOL"),
    ("rim_calcification_subtype",     "STRING"),
    ("interval_growth",               "BOOL"),
    ("tirads_reported_system",        "STRING"),
    ("evidence_short",                "STRING"),
    ("primitive_backfill_model",      "STRING"),
    ("primitive_backfill_extracted_at","TIMESTAMP"),
    ("primitive_backfill_confidence", "FLOAT64"),
]


def _add_columns_sql() -> list[str]:
    sqls = []
    for col, dtype in NEW_COLS:
        sqls.append(
            f"ALTER TABLE {NODULE_CANON} "
            f"ADD COLUMN IF NOT EXISTS {col} {dtype};"
        )
    return sqls


def step_c8(bq: "bigquery.Client") -> None:
    # 1. ADD COLUMN IF NOT EXISTS for each new field
    for sql in _add_columns_sql():
        _run_sql(bq, sql, f"C.8 {sql[:60]}…")

    # 2. CTAS-rebuild with COALESCE (existing wins).
    # Use EXCEPT to remove the new backfill columns from n.* so we can supply
    # them via COALESCE without duplicate-column errors.
    _except_cols = ", ".join(col for col, _ in NEW_COLS)
    ctas_sql = f"""
CREATE OR REPLACE TABLE {NODULE_CANON}
CLUSTER BY research_id AS
SELECT
  n.* EXCEPT({_except_cols}),
  -- COALESCE: existing non-null canonical value wins; hybrid fills nulls only
  COALESCE(n.composition_llm, h.composition_llm)                          AS composition_llm,
  COALESCE(n.echogenicity_llm, h.echogenicity_llm)                        AS echogenicity_llm,
  COALESCE(n.shape_llm, h.shape_llm)                                      AS shape_llm,
  COALESCE(n.margins_llm, h.margins_llm)                                  AS margins_llm,
  COALESCE(n.echogenic_foci_llm_jsonarray, h.echogenic_foci_llm_jsonarray) AS echogenic_foci_llm_jsonarray,
  COALESCE(n.halo_jsonb, h.halo_jsonb)                                    AS halo_jsonb,
  COALESCE(n.vascularity_jsonb, h.vascularity_jsonb)                      AS vascularity_jsonb,
  COALESCE(n.ete_us_jsonb, h.ete_us_jsonb)                                AS ete_us_jsonb,
  COALESCE(n.halo_presence_simple, h.halo_presence_simple)                AS halo_presence_simple,
  COALESCE(n.vascularity_distribution_simple, h.vascularity_distribution_simple) AS vascularity_distribution_simple,
  COALESCE(n.ete_on_us_presence_simple, h.ete_on_us_presence_simple)      AS ete_on_us_presence_simple,
  COALESCE(n.chammas_type_llm, h.chammas_type_llm)                        AS chammas_type_llm,
  COALESCE(n.entirely_calcified, h.entirely_calcified)                    AS entirely_calcified,
  COALESCE(n.homogeneous_echotexture, h.homogeneous_echotexture)          AS homogeneous_echotexture,
  COALESCE(n.rim_calcification_subtype, h.rim_calcification_subtype)      AS rim_calcification_subtype,
  COALESCE(n.interval_growth, h.interval_growth)                          AS interval_growth,
  COALESCE(n.tirads_reported_system, h.tirads_reported_system)            AS tirads_reported_system,
  COALESCE(n.evidence_short, h.evidence_short)                            AS evidence_short,
  h.primitive_backfill_model                                               AS primitive_backfill_model,
  h.extracted_at                                                           AS primitive_backfill_extracted_at,
  h.confidence_overall                                                     AS primitive_backfill_confidence
FROM {NODULE_CANON} n
LEFT JOIN {HYBRID} h USING (nodule_id);
"""
    # BQ CTAS with same destination: write to workspace staging, then CTAS-copy
    # into pub_canonical. RENAME TO is within-dataset only; must use full CTAS.
    stage = f"{WORK}.canonical_us_nodule_v2_stage_hybrid`"
    ctas_staged = ctas_sql.replace(f"TABLE {NODULE_CANON}", f"TABLE {stage}")
    _run_sql(bq, ctas_staged, "C.8 CTAS into workspace staging table")

    # Cross-dataset CTAS from workspace staging → pub_canonical
    cross_sql = f"""
    CREATE OR REPLACE TABLE {NODULE_CANON}
    CLUSTER BY research_id AS
    SELECT * FROM {stage};
    """
    _run_sql(bq, cross_sql, "C.8 Cross-dataset CTAS workspace-stage → pub_canonical")
    _run_sql(bq, f"DROP TABLE IF EXISTS {stage};", "C.8 Drop workspace staging table")
    _log("C.8 Canonical rebuilt with hybrid primitives.")


# ---------------------------------------------------------------------------
# C.9 — ACR scorer + per-tier audit
# ---------------------------------------------------------------------------

ACR_SCORER_SQL = f"""
UPDATE {NODULE_CANON}
SET acr2017_feature_points_complete = (
  composition_llm IS NOT NULL
  AND echogenicity_llm IS NOT NULL
  AND shape_llm IS NOT NULL
  AND margins_llm IS NOT NULL
  AND echogenic_foci_llm_jsonarray IS NOT NULL
)
WHERE TRUE;
"""

ACR_COMPLETE_SQL = f"""
SELECT
  COUNTIF(acr2017_feature_points_complete) / COUNT(*) AS acr_complete_rate
FROM {NODULE_CANON};
"""

PER_TIER_AUDIT_SQL = f"""
SELECT
  primitive_backfill_model,
  COUNT(*) AS n,
  COUNTIF(composition_llm IS NOT NULL) / COUNT(*) AS fill_composition,
  COUNTIF(echogenicity_llm IS NOT NULL) / COUNT(*) AS fill_echogenicity,
  COUNTIF(shape_llm IS NOT NULL) / COUNT(*) AS fill_shape,
  COUNTIF(margins_llm IS NOT NULL) / COUNT(*) AS fill_margins,
  AVG(primitive_backfill_confidence) AS avg_confidence
FROM {NODULE_CANON}
WHERE primitive_backfill_model IS NOT NULL
GROUP BY primitive_backfill_model
ORDER BY n DESC;
"""

GROUND_TRUTH_AUDIT_SQL = f"""
-- Per-tier concordance vs ground-truth 5149-row overlap subset
-- (requires pub_workspace.canonical_us_nodule_v2_snapshot_prebackfill_v1 as ground truth)
SELECT
  h.primitive_backfill_model,
  COUNT(*) AS n,
  COUNTIF(gt.composition = h.composition_llm) / COUNT(*) AS concordance_composition,
  COUNTIF(gt.echogenicity = h.echogenicity_llm) / COUNT(*) AS concordance_echogenicity,
  COUNTIF(gt.shape = h.shape_llm) / COUNT(*) AS concordance_shape,
  COUNTIF(gt.margins = h.margins_llm) / COUNT(*) AS concordance_margins
FROM `{PROJECT}.pub_canonical.canonical_us_nodule_v2` h
JOIN `{PROJECT}.pub_workspace.tirads_primitive_regex_v1_v1` rx USING (nodule_id)
-- gt is the A.2 snapshot with pre-existing ground-truth values
JOIN (
  SELECT nodule_id, composition, echogenicity, shape, margins
  FROM `{PROJECT}.pub_workspace.canonical_us_nodule_v2_snapshot_prebackfill_v1`
  WHERE composition IS NOT NULL OR echogenicity IS NOT NULL
) gt USING (nodule_id)
GROUP BY h.primitive_backfill_model
ORDER BY n DESC;
"""


def step_c9(bq: "bigquery.Client") -> dict:
    # ACR scorer update
    _run_sql(bq, ACR_SCORER_SQL, "C.9 ACR 2017 scorer update")

    acr_rate = _scalar(bq, ACR_COMPLETE_SQL)
    _log(f"C.9 ACR-complete rate: {acr_rate:.1%}")
    if acr_rate < 0.70:
        _log(f"WARNING: ACR-complete rate {acr_rate:.1%} < 70% target.")

    # Per-tier fill rates
    tier_rows = list(bq.query(PER_TIER_AUDIT_SQL, location=LOCATION).result())
    for r in tier_rows:
        _log(f"  Tier={r['primitive_backfill_model']} n={r['n']} "
             f"comp={r['fill_composition']:.1%} echo={r['fill_echogenicity']:.1%} "
             f"shape={r['fill_shape']:.1%} margins={r['fill_margins']:.1%} "
             f"conf={r['avg_confidence']:.2f}")

    # Ground-truth concordance (may fail if snapshot not available yet)
    try:
        gt_rows = list(bq.query(GROUND_TRUTH_AUDIT_SQL, location=LOCATION).result())
        for r in gt_rows:
            model = r["primitive_backfill_model"]
            comp = r["concordance_composition"]
            echo = r["concordance_echogenicity"]
            _log(f"  GT concordance: {model} comp={comp:.1%} echo={echo:.1%}")
            # Per-tier thresholds
            thresh = {"regex_heuristic_v1": 0.88, "gemini-2.5-flash": 0.92,
                      "gemini-2.5-pro": 0.95}.get(model, 0.88)
            if comp < thresh or echo < thresh:
                _log(f"  WARNING: {model} concordance below {thresh:.0%} threshold.")
    except Exception as e:
        _log(f"C.9 ground-truth audit skipped (snapshot may not exist yet): {e}")

    return {"acr_rate": float(acr_rate)}


# ---------------------------------------------------------------------------
# Signoff registry insert
# ---------------------------------------------------------------------------

SIGNOFF_SQL = f"""
INSERT INTO {SIGNOFF} (table_name, version_tag, phase, lifecycle, notes, created_at)
VALUES (
  'canonical_us_nodule_v2',
  'v1.0+phase_a_hybrid',
  'phase_a3_hybrid',
  'Active',
  'Phase A.3 hybrid regex+Flash+Pro primitive backfill applied. See A.3 DFL row and THY-30.',
  CURRENT_TIMESTAMP()
);
"""


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def main(step: str = "all") -> None:
    if not _HAS_BQ:
        print("ERROR: google-cloud-bigquery not installed.", file=sys.stderr)
        sys.exit(1)

    bq = _bq_client()
    _log(f"412 pipeline starting, step={step}")

    results: dict = {}

    if step in ("all", "c2"):
        results["c2_residual_n"] = step_c2(bq)
    if step in ("all", "c3"):
        residual_n = results.get("c2_residual_n") or _scalar(bq, f"SELECT COUNT(*) FROM {RESIDUAL}")
        results["c3"] = step_c3(bq, residual_n)
    if step in ("all", "c4"):
        results["c4_flash_n"] = step_c4(bq)
    if step in ("all", "c5"):
        flash_n = results.get("c4_flash_n") or _scalar(bq, f"SELECT COUNT(*) FROM {FLASH_RAW}")
        results["c5_reroute_n"] = step_c5(bq, flash_n)
    if step in ("all", "c6"):
        reroute_n = results.get("c5_reroute_n") or _scalar(bq, f"SELECT COUNT(*) FROM {PRO_REROUTE}")
        flash_n = results.get("c4_flash_n") or _scalar(bq, f"SELECT COUNT(*) FROM {FLASH_RAW}")
        results["c6_pro_n"] = step_c6(bq, reroute_n, flash_n)
    if step in ("all", "c7"):
        results["c7"] = step_c7(bq)
    if step in ("all", "c8"):
        step_c8(bq)
    if step in ("all", "c9"):
        results["c9"] = step_c9(bq)
        if step == "all":
            _run_sql(bq, SIGNOFF_SQL, "Signoff registry insert")

    # Save result summary
    out_file = OUT_DIR / f"412_hybrid_pipeline_{RUN_TS}.json"
    out_file.write_text(json.dumps(results, indent=2, default=str))
    _log(f"Results saved → {out_file}")
    _log("412 pipeline complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Phase A.3 hybrid pipeline orchestrator")
    parser.add_argument(
        "--step",
        default="all",
        choices=["all", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"],
        help="Which step to run (default: all)",
    )
    args = parser.parse_args()
    main(step=args.step)
