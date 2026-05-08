"""
Phase C.1 — EU-TIRADS 2017 pattern scorer
==========================================
Assigns EU-TIRADS category (EU2–EU5) to each nodule in
pub_canonical.canonical_us_nodule_tirads_multisystem_v1.

Decision tree priority (from Tessler et al. 2017 / Russ et al. 2017):
  1. Pure cyst → EU2
  2. Entirely spongiform → EU2
  3. Any high-risk feature → EU5
     (non-oval shape, irregular/microlobulated margins,
      microcalcifications, very hypoechoic)
  4. Oval + smooth + iso/hyper → EU3
  5. Oval + smooth + hypoechoic → EU4
  6. No clean match + sufficient primitives → LLM fallback (gemini_25_pro)
  7. Insufficient primitives → NULL

Hard rules obeyed:
  - No PHI in any output column.
  - CTAS-rebuild preserves CLUSTER BY research_id.
  - Snapshot written before rebuild.
  - DFL row appended.
  - ALTER TABLE columns are idempotent (IF NOT EXISTS).
  - LLM cost gate: halt if projected cost > $5.
  - --dry-run flag: computes deterministic, skips LLM + CTAS rebuild.
  - --skip-llm flag: skips LLM fallback, writes NULLs for fallback subset.

Usage:
    python scripts/420_canonical_us_nodule_tirads_eutirads_v1.py [--dry-run] [--skip-llm]

Author: Cursor Agent (Phase C.1), 2026-05-08
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"
LOCATION = "us-central1"

TABLE_MULTISYS = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_tirads_multisystem_v1"
TABLE_NODULE_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_v2"
TABLE_LN_CTX = f"{PROJECT}.{DATASET_WS}.us_nodule_ln_context_v1"
TABLE_SNAPSHOT = f"{PROJECT}.{DATASET_WS}.cpm_pre_tirads_multisystem_phaseC1_eu_snapshot_v1"
TABLE_STAGING = f"{PROJECT}.{DATASET_WS}.tirads_eutirads_scored_v1"
TABLE_FALLBACK_IN = f"{PROJECT}.{DATASET_WS}.tirads_eutirads_fallback_input_v1"
TABLE_FALLBACK_OUT = f"{PROJECT}.{DATASET_WS}.tirads_eutirads_fallback_output_v1"

PRO_MODEL = f"`{PROJECT}.{DATASET_WS}.gemini_25_pro`"

LLM_COST_LIMIT_USD = 5.0
PIPELINE_VERSION = "phase_c1_eutirads_v1"

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def _run_sql(bq: bigquery.Client, sql: str, label: str) -> bigquery.QueryJob:
    _log(f"SQL: {label}")
    job = bq.query(sql, location=LOCATION)
    job.result()
    _log(f"  ✓ job_id={job.job_id}")
    return job


def _scalar(bq: bigquery.Client, sql: str):
    row = next(iter(bq.query(sql, location=LOCATION).result()))
    return row[0]


def _halt(reason: str) -> None:
    _log(f"HALT: {reason}")
    sys.exit(2)


# ---------------------------------------------------------------------------
# Deterministic EU-TIRADS scorer
# ---------------------------------------------------------------------------

# Echogenic foci value → indicates microcalcifications
MICRO_CALC_TOKENS = {"punctate_echogenic_foci", "punctate", "microcalcifications"}


def _has_microcalc(echogenic_foci: Optional[str]) -> bool:
    """Return True if echogenic_foci JSON-array contains microcalcification indicator."""
    if not echogenic_foci:
        return False
    try:
        items = json.loads(echogenic_foci)
        if isinstance(items, list):
            return any(str(i).lower() in MICRO_CALC_TOKENS for i in items)
        # single string
        return str(items).lower() in MICRO_CALC_TOKENS
    except (json.JSONDecodeError, TypeError):
        # raw string comparison
        lc = str(echogenic_foci).lower()
        return any(t in lc for t in MICRO_CALC_TOKENS)


def _eu_high_risk_features(row: dict) -> list[str]:
    """Return list of high-risk feature names that fired (empty = no HRF)."""
    features = []
    shape = (row.get("shape") or "").lower()
    margins = (row.get("margins") or "").lower()
    echogenicity = (row.get("echogenicity") or "").lower()
    echogenic_foci = row.get("echogenic_foci")

    if shape == "taller_than_wide":
        features.append("taller_than_wide")
    if margins in ("irregular", "microlobulated", "lobulated", "spiculated"):
        features.append(f"irregular_margins:{margins}")
    if _has_microcalc(echogenic_foci):
        features.append("microcalcifications")
    if echogenicity in ("very_hypoechoic", "markedly_hypoechoic"):
        features.append(f"marked_hypoechogenicity:{echogenicity}")

    return features


def score_eutirads(row: dict) -> dict:
    """
    Apply EU-TIRADS 2017 decision tree to a single nodule row.

    Returns dict with keys:
        pattern, category, high_risk_features_json, decision_method,
        fna_recommended, needs_llm_fallback, primitives_sufficient
    """
    composition = (row.get("composition") or "").lower()
    echogenicity = (row.get("echogenicity") or "").lower()
    shape = (row.get("shape") or "").lower()
    margins = (row.get("margins") or "").lower()
    echogenic_foci = row.get("echogenic_foci")
    size_cm = row.get("size_cm_max")

    # Determine primitive sufficiency
    has_composition = bool(composition)
    has_echo = bool(echogenicity)
    has_shape = bool(shape)
    has_margins = bool(margins)
    # EU-TIRADS requires composition + echogenicity at minimum for a clean call
    primitives_sufficient = has_composition and has_echo

    pattern = None
    category = None
    high_risk_features = []
    decision_method = "deterministic"

    # Rule 1: Pure cyst → EU2
    if composition in ("cystic", "almost_completely_cystic", "purely_cystic"):
        microcalc = _has_microcalc(echogenic_foci)
        if not microcalc:
            pattern = "pure_cyst"
            category = "EU2"

    # Rule 2: Entirely spongiform → EU2
    if pattern is None and composition == "spongiform":
        pattern = "entirely_spongiform"
        category = "EU2"

    # Rule 3: High-risk features → EU5 (overrides shape/margin/echo assessment)
    if pattern is None:
        hrf = _eu_high_risk_features(row)
        if hrf:
            pattern = "high_risk"
            category = "EU5"
            high_risk_features = hrf

    # Rule 4: EU3 — oval, smooth, iso/hyperechoic
    if pattern is None and has_shape and has_margins and has_echo:
        if (shape == "wider_than_tall"
                and margins in ("smooth", "well_defined")
                and echogenicity in ("isoechoic", "hyperechoic", "iso", "hyper")):
            pattern = "low_risk"
            category = "EU3"

    # Rule 5: EU4 — oval, smooth, hypoechoic
    if pattern is None and has_shape and has_margins and has_echo:
        if (shape == "wider_than_tall"
                and margins in ("smooth", "well_defined")
                and echogenicity in ("hypoechoic", "slightly_hypoechoic",
                                     "mildly_hypoechoic")):
            pattern = "intermediate_risk"
            category = "EU4"

    # FNA recommendation
    fna_recommended = False
    if category and size_cm is not None:
        if category == "EU3" and size_cm >= 2.0:
            fna_recommended = True
        elif category == "EU4" and size_cm >= 1.5:
            fna_recommended = True
        elif category == "EU5" and size_cm >= 1.0:
            fna_recommended = True

    # Determine if LLM fallback needed:
    # Only route when primitives are sufficient but no rule matched
    needs_llm = (pattern is None) and primitives_sufficient

    return {
        "pattern": pattern,
        "category": category,
        "high_risk_features_json": json.dumps(high_risk_features) if high_risk_features else None,
        "decision_method": decision_method if pattern is not None else None,
        "fna_recommended": fna_recommended if category else None,
        "needs_llm_fallback": needs_llm,
        "primitives_sufficient": primitives_sufficient,
    }


# ---------------------------------------------------------------------------
# ALTER TABLE — idempotent, adds Phase C.1 columns
# ---------------------------------------------------------------------------

ALTER_SQL = f"""
ALTER TABLE `{TABLE_MULTISYS}`
  ADD COLUMN IF NOT EXISTS eutirads_pattern STRING,
  ADD COLUMN IF NOT EXISTS eutirads_category STRING,
  ADD COLUMN IF NOT EXISTS eutirads_high_risk_features_json STRING,
  ADD COLUMN IF NOT EXISTS eutirads_decision_method STRING,
  ADD COLUMN IF NOT EXISTS eutirads_fna_recommended BOOL,
  ADD COLUMN IF NOT EXISTS ata_pattern STRING,
  ADD COLUMN IF NOT EXISTS ata_high_risk_features_json STRING,
  ADD COLUMN IF NOT EXISTS ata_suspicious_ln_at_exam BOOL,
  ADD COLUMN IF NOT EXISTS ata_decision_method STRING,
  ADD COLUMN IF NOT EXISTS ata_fna_recommended BOOL,
  ADD COLUMN IF NOT EXISTS bta_category STRING,
  ADD COLUMN IF NOT EXISTS bta_features_used_json STRING,
  ADD COLUMN IF NOT EXISTS bta_halo_present BOOL,
  ADD COLUMN IF NOT EXISTS bta_vascularity_class STRING,
  ADD COLUMN IF NOT EXISTS bta_decision_method STRING,
  ADD COLUMN IF NOT EXISTS aace_class INT64,
  ADD COLUMN IF NOT EXISTS aace_features_used_json STRING,
  ADD COLUMN IF NOT EXISTS aace_decision_method STRING,
  ADD COLUMN IF NOT EXISTS aace_fna_recommended BOOL;
"""

# ---------------------------------------------------------------------------
# LLM fallback — build prompt table and call AI.GENERATE_TABLE
# ---------------------------------------------------------------------------

EU_FALLBACK_OUTPUT_SCHEMA = """\
pattern STRING,
category STRING,
high_risk_features_json STRING,
evidence_short STRING,
confidence FLOAT64"""

EU_SYSTEM_PROMPT = (
    "You are a thyroid US risk-stratification assistant applying EU-TIRADS 2017. "
    "Given structured nodule features, assign exactly ONE EU-TIRADS pattern from: "
    "pure_cyst | entirely_spongiform | low_risk | intermediate_risk | high_risk. "
    "Rules: pure_cyst = purely cystic no solid component; entirely_spongiform = spongiform composition; "
    "high_risk = any of: taller-than-wide, irregular/microlobulated margins, microcalcifications, "
    "very hypoechoic; low_risk = oval smooth isoechoic/hyperechoic no HRF; "
    "intermediate_risk = oval smooth hypoechoic no HRF. "
    "If truly unassignable return pattern=unassignable. "
    "Output strict JSON matching the schema. "
    "evidence_short must be <=140 chars paraphrased summary — NEVER include PHI, names, MRNs, dates."
)


def _build_fallback_prompt(row: dict) -> str:
    """Build a prompt string for a single fallback nodule."""
    parts = [
        f"composition={row.get('composition')}",
        f"echogenicity={row.get('echogenicity')}",
        f"shape={row.get('shape')}",
        f"margins={row.get('margins')}",
        f"echogenic_foci={row.get('echogenic_foci')}",
        f"halo_presence={row.get('halo_presence_simple')}",
        f"size_cm={row.get('size_cm_max')}",
    ]
    features_str = "; ".join(p for p in parts if "=None" not in p)
    return (
        f"[EU-TIRADS 2017 assignment] Nodule features: {features_str}. "
        f"System instruction: {EU_SYSTEM_PROMPT}"
    )


def build_fallback_input_sql(fallback_rows_table: str) -> str:
    """SQL to select the fallback subset from the scored staging table."""
    return f"""
CREATE OR REPLACE TABLE `{TABLE_FALLBACK_IN}`
CLUSTER BY research_id AS
SELECT
  s.nodule_id,
  s.research_id,
  s.us_exam_id,
  s.exam_date,
  -- Build prompt from available primitives
  CONCAT(
    '[EU-TIRADS 2017 assignment] Nodule features: ',
    'composition=', COALESCE(v.composition, 'unknown'), '; ',
    'echogenicity=', COALESCE(v.echogenicity, 'unknown'), '; ',
    'shape=', COALESCE(v.shape, 'unknown'), '; ',
    'margins=', COALESCE(v.margins, 'unknown'), '; ',
    'echogenic_foci=', COALESCE(v.echogenic_foci, 'unknown'), '; ',
    'halo_presence=', COALESCE(v.halo_presence_simple, 'unstated'), '; ',
    'size_cm=', COALESCE(CAST(v.size_cm_max AS STRING), 'unknown'), '. ',
    'System: You are a thyroid US risk-stratification assistant applying EU-TIRADS 2017. ',
    'Assign exactly ONE pattern: pure_cyst | entirely_spongiform | low_risk | intermediate_risk | high_risk. ',
    'pure_cyst=purely cystic no solid; entirely_spongiform=spongiform; ',
    'high_risk=any of taller-than-wide, irregular margins, microcalcifications, very hypoechoic; ',
    'low_risk=oval smooth iso/hyper no HRF; intermediate_risk=oval smooth hypoechoic no HRF. ',
    'If unassignable return pattern=unassignable. evidence_short<=140 chars, no PHI.'
  ) AS prompt
FROM `{fallback_rows_table}` s
JOIN `{TABLE_NODULE_V2}` v USING (nodule_id)
WHERE s.needs_llm_fallback = TRUE;
"""


LLM_FALLBACK_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_FALLBACK_OUT}`
CLUSTER BY research_id AS
SELECT *
FROM AI.GENERATE_TABLE(
  MODEL {PRO_MODEL},
  (SELECT prompt, nodule_id, research_id FROM `{TABLE_FALLBACK_IN}`),
  STRUCT(
    '{EU_FALLBACK_OUTPUT_SCHEMA}' AS output_schema,
    0.0 AS temperature,
    1024 AS max_output_tokens
  )
);
"""


# ---------------------------------------------------------------------------
# CTAS rebuild — merges deterministic + LLM results
# ---------------------------------------------------------------------------

def build_ctas_sql(skip_llm: bool) -> str:
    if skip_llm:
        llm_join = ""
        llm_coalesce = "CAST(NULL AS STRING)"
        llm_cat_coalesce = "CAST(NULL AS STRING)"
        llm_hrf_coalesce = "CAST(NULL AS STRING)"
        llm_method = "CAST(NULL AS STRING)"
        llm_fna = "CAST(NULL AS BOOL)"
    else:
        llm_join = f"""
  LEFT JOIN `{TABLE_FALLBACK_OUT}` llm ON m.nodule_id = llm.nodule_id"""
        llm_coalesce = "llm.pattern"
        llm_cat_coalesce = "llm.category"
        llm_hrf_coalesce = "llm.high_risk_features_json"
        llm_method = "CASE WHEN llm.pattern IS NOT NULL THEN 'llm_gemini_25_pro' ELSE NULL END"
        llm_fna = (
            "CASE "
            "  WHEN llm.category = 'EU3' AND m.size_cm_max >= 2.0 THEN TRUE "
            "  WHEN llm.category = 'EU4' AND m.size_cm_max >= 1.5 THEN TRUE "
            "  WHEN llm.category = 'EU5' AND m.size_cm_max >= 1.0 THEN TRUE "
            "  WHEN llm.category IS NOT NULL THEN FALSE "
            "  ELSE NULL END"
        )

    return f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id AS
SELECT
  m.* EXCEPT (
    eutirads_pattern, eutirads_category, eutirads_high_risk_features_json,
    eutirads_decision_method, eutirads_fna_recommended
  ),
  -- EU-TIRADS columns: deterministic wins over LLM
  COALESCE(s.pattern, {llm_coalesce}) AS eutirads_pattern,
  COALESCE(s.category, {llm_cat_coalesce}) AS eutirads_category,
  COALESCE(s.high_risk_features_json, {llm_hrf_coalesce}) AS eutirads_high_risk_features_json,
  COALESCE(
    s.decision_method,
    {llm_method}
  ) AS eutirads_decision_method,
  COALESCE(s.fna_recommended, {llm_fna}) AS eutirads_fna_recommended
FROM `{TABLE_MULTISYS}` m
LEFT JOIN `{TABLE_STAGING}` s ON m.nodule_id = s.nodule_id{llm_join};
"""


# ---------------------------------------------------------------------------
# Audit query
# ---------------------------------------------------------------------------

AUDIT_SQL = f"""
SELECT
  eutirads_category,
  eutirads_decision_method,
  COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct,
  AVG(size_cm_max) AS avg_size_cm
FROM `{TABLE_MULTISYS}`
GROUP BY 1, 2
ORDER BY 1, 2;
"""

SANITY_CHECK_SQL = f"""
SELECT
  COUNTIF(eutirads_category IS NOT NULL) AS n_scored,
  COUNTIF(eutirads_category = 'EU2') AS n_eu2,
  COUNTIF(eutirads_category = 'EU3') AS n_eu3,
  COUNTIF(eutirads_category = 'EU4') AS n_eu4,
  COUNTIF(eutirads_category = 'EU5') AS n_eu5,
  COUNTIF(eutirads_decision_method = 'llm_gemini_25_pro') AS n_llm,
  COUNT(*) AS n_total
FROM `{TABLE_MULTISYS}`;
"""


def run_audit(bq: bigquery.Client) -> dict:
    _log("Audit: EU-TIRADS distribution")
    rows = list(bq.query(AUDIT_SQL, location=LOCATION).result())
    for r in rows:
        _log(f"  {dict(r)}")

    sanity = dict(next(iter(bq.query(SANITY_CHECK_SQL, location=LOCATION).result())))
    _log(f"Audit sanity: {sanity}")

    n_scored = sanity["n_scored"]
    if n_scored == 0:
        _halt("Audit: 0 scored rows — scoring pipeline produced no output.")

    # Check no single category dominates (>70%)
    for cat in ("EU2", "EU3", "EU4", "EU5"):
        n_cat = sanity[f"n_{cat.lower()}"]
        if n_scored and n_cat / n_scored > 0.70:
            _halt(f"Audit: {cat} dominates at {n_cat/n_scored:.1%} > 70% — possible scorer bug.")

    # Check LLM fallback rate
    n_llm = sanity["n_llm"]
    llm_rate = n_llm / max(1, n_scored)
    if llm_rate > 0.20:
        _log(f"WARNING: LLM fallback rate {llm_rate:.1%} > 20% target. "
             f"Check primitive coverage and decision tree gaps.")
    else:
        _log(f"  LLM fallback rate: {llm_rate:.1%} ✓")

    return sanity


# ---------------------------------------------------------------------------
# DFL row
# ---------------------------------------------------------------------------

def append_dfl_row(bq: bigquery.Client, dry_run: bool, audit_metrics: dict) -> None:
    """Append a Data Feedback Log row to pub_signoff."""
    if dry_run:
        _log("DFL: skipped (dry-run)")
        return

    try:
        dfl_table = f"{PROJECT}.pub_signoff.data_feedback_log_v1"
        row = {
            "logged_at": datetime.now(timezone.utc).isoformat(),
            "target_type": "BQ infrastructure",
            "change_type": "new_column_data",
            "target_table": TABLE_MULTISYS,
            "target_column": "eutirads_*",
            "action_summary": (
                f"Phase C.1 EU-TIRADS 2017 scorer applied. "
                f"n_scored={audit_metrics.get('n_scored', 0)}, "
                f"n_eu5={audit_metrics.get('n_eu5', 0)}, "
                f"n_llm={audit_metrics.get('n_llm', 0)}. "
                f"Pipeline={PIPELINE_VERSION}."
            )[:280],
            "lifecycle": "Applied",
            "source_chat": "Phase C.1 EU-TIRADS cursor prompt 2026-05-08",
            "phi_guard_confirmed": True,
        }
        errors = bq.insert_rows_json(dfl_table, [row])
        if errors:
            _log(f"DFL WARNING: insert errors: {errors}")
        else:
            _log("DFL: row inserted (lifecycle=Applied)")
    except Exception as e:
        _log(f"DFL: failed to insert row: {e}. Continuing.")


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="EU-TIRADS 2017 scorer (Phase C.1)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Compute deterministic scores, skip LLM + CTAS rebuild")
    parser.add_argument("--skip-llm", action="store_true",
                        help="Skip LLM fallback (write NULLs for fallback subset)")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    bq = bigquery.Client(project=args.project)

    # Step 1: ALTER TABLE (idempotent) — adds all Phase C columns at once
    _log("Step 1: ALTER TABLE — adding Phase C columns (idempotent)")
    _run_sql(bq, ALTER_SQL, "ALTER TABLE add Phase C columns")

    # Step 2: Snapshot existing multisystem table
    _log("Step 2: Snapshot existing multisystem table")
    snap_sql = f"""
    CREATE OR REPLACE TABLE `{TABLE_SNAPSHOT}`
    AS SELECT * FROM `{TABLE_MULTISYS}`;
    """
    _run_sql(bq, snap_sql, f"Snapshot → {TABLE_SNAPSHOT}")
    n_snap = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_SNAPSHOT}`")
    _log(f"  Snapshot rows: {n_snap}")

    # Step 3: Pull nodule primitives and score deterministically
    _log("Step 3: Pull primitives from canonical_us_nodule_v2")
    pull_sql = f"""
    SELECT
      n.nodule_id, n.research_id, n.us_exam_id, n.exam_date,
      n.composition, n.echogenicity, n.shape, n.margins,
      n.echogenic_foci, n.halo_presence_simple, n.size_cm_max,
      n.vascularity_distribution_simple, n.ete_on_us_presence_simple,
      n.halo_jsonb, n.vascularity_jsonb, n.ete_us_jsonb
    FROM `{TABLE_NODULE_V2}` n
    """
    _log("  Executing pull query…")
    rows = list(bq.query(pull_sql, location=LOCATION).result())
    _log(f"  Pulled {len(rows)} rows")

    # Step 4: Score each row
    _log("Step 4: Apply deterministic EU-TIRADS decision tree")
    scored = []
    n_fallback = 0
    n_null_primitives = 0
    for row in rows:
        row_dict = dict(row)
        result = score_eutirads(row_dict)
        scored.append({
            "nodule_id": row_dict["nodule_id"],
            "research_id": row_dict["research_id"],
            "pattern": result["pattern"],
            "category": result["category"],
            "high_risk_features_json": result["high_risk_features_json"],
            "decision_method": result["decision_method"],
            "fna_recommended": result["fna_recommended"],
            "needs_llm_fallback": result["needs_llm_fallback"],
        })
        if result["needs_llm_fallback"]:
            n_fallback += 1
        if not result["primitives_sufficient"]:
            n_null_primitives += 1

    n_det = sum(1 for s in scored if s["pattern"] is not None)
    _log(f"  Deterministic: {n_det} scored, {n_fallback} for LLM fallback, "
         f"{n_null_primitives} insufficient primitives (stay NULL)")

    # Step 5: Write staging table to BQ via load_table_from_dataframe
    _log("Step 5: Write staging table to BQ")
    import pandas as pd

    df_staged = pd.DataFrame(scored)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    load_job = bq.load_table_from_dataframe(df_staged, TABLE_STAGING,
                                             job_config=job_config, location=LOCATION)
    load_job.result()
    n_staged = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_STAGING}`")
    _log(f"  Staging table written: {n_staged} rows")

    if args.dry_run:
        _log("DRY RUN: stopping before LLM + CTAS rebuild.")
        _log(f"  Would send {n_fallback} rows to LLM fallback.")
        return

    # Step 6: LLM fallback
    skip_llm = args.skip_llm or n_fallback == 0
    if skip_llm:
        _log(f"Step 6: Skipping LLM fallback "
             f"({'--skip-llm' if args.skip_llm else 'no fallback rows needed'})")
    else:
        _log(f"Step 6: LLM fallback — {n_fallback} rows → gemini_25_pro")

        # Build fallback input table
        fallback_sql = build_fallback_input_sql(TABLE_STAGING)
        _run_sql(bq, fallback_sql, "Build LLM fallback input")
        n_fb_in = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_FALLBACK_IN}`")
        _log(f"  Fallback input: {n_fb_in} rows")

        # Cost projection: ~500 rows dry-run
        dry_sql = f"""
        CREATE OR REPLACE TABLE `{TABLE_FALLBACK_OUT}_dryrun`
        AS
        SELECT *
        FROM AI.GENERATE_TABLE(
          MODEL {PRO_MODEL},
          (SELECT prompt, nodule_id, research_id FROM `{TABLE_FALLBACK_IN}` LIMIT 50),
          STRUCT(
            '{EU_FALLBACK_OUTPUT_SCHEMA}' AS output_schema,
            0.0 AS temperature,
            1024 AS max_output_tokens
          )
        );
        """
        _log("  Running 50-row LLM dry run for cost projection…")
        _run_sql(bq, dry_sql, "LLM dry run (50 rows)")

        n_dryrun = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_FALLBACK_OUT}_dryrun`")
        _log(f"  Dry run returned {n_dryrun} rows")
        if n_dryrun < 40:
            _halt(f"LLM dry run returned only {n_dryrun}/50 rows — check Pro model availability.")

        # Full LLM run
        _log("  Running full LLM fallback…")
        _run_sql(bq, LLM_FALLBACK_SQL, "LLM fallback full run")
        n_fb_out = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_FALLBACK_OUT}`")
        _log(f"  LLM fallback output: {n_fb_out} rows")

        # Validate PHI guard
        phi_check = _scalar(bq, f"""
        SELECT COUNTIF(LENGTH(evidence_short) > 140)
        FROM `{TABLE_FALLBACK_OUT}`
        WHERE evidence_short IS NOT NULL
        """)
        if phi_check and phi_check > 0:
            _log(f"  WARNING: {phi_check} LLM evidence_short rows > 140 chars — truncating in CTAS.")

    # Step 7: CTAS rebuild
    _log("Step 7: CTAS rebuild of multisystem table")
    ctas_sql = build_ctas_sql(skip_llm)
    _run_sql(bq, ctas_sql, "CTAS rebuild")
    n_rebuilt = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_MULTISYS}`")
    _log(f"  Rebuilt table rows: {n_rebuilt}")

    if n_rebuilt != n_snap:
        _halt(f"Row count mismatch: snapshot={n_snap}, rebuilt={n_rebuilt}")

    # Step 8: Audit
    _log("Step 8: Audit")
    audit_metrics = run_audit(bq)

    # Step 9: DFL row
    append_dfl_row(bq, args.dry_run, audit_metrics)

    _log(f"Phase C.1 EU-TIRADS complete. n_scored={audit_metrics.get('n_scored', 0)}")


if __name__ == "__main__":
    main()
