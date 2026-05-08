"""
Phase C.2 — ATA 2015 pattern scorer
=====================================
Assigns ATA 2015 sonographic pattern (benign / very_low / low /
intermediate / high) to each nodule in
pub_canonical.canonical_us_nodule_tirads_multisystem_v1.

Decision tree per ATA 2015 / Haugen et al. (Thyroid 2016):
  1. Purely cystic, no solid component → benign
  2. Spongiform OR (mixed/predominantly_cystic + no suspicious features) → very_low
  3. Solid/predominantly_solid hypoechoic + ≥1 high-risk feature → high
  4. Mixed/predominantly_cystic + hypoechoic solid component + ≥1 HRF → high
  5. Solid/predominantly_solid hypoechoic + smooth margins + no microcalc/ETE/taller-than-wide → intermediate
  6. Solid/predominantly_solid iso/hyperechoic + no suspicious features → low
  7. Mixed/predominantly_cystic + no other suspicious features → low
  8. No clean match + sufficient primitives → LLM fallback
  9. Insufficient primitives → NULL

LN modifier: has_suspicious_ln_within_60d=TRUE triggers FNA consideration
for otherwise borderline-FNA subcentimeter nodules.

Hard rules obeyed:
  - No PHI in any output column.
  - CTAS-rebuild preserves CLUSTER BY research_id (EXCEPT clause for ATA cols).
  - Snapshot written before rebuild.
  - DFL row appended.
  - --dry-run flag skips LLM + CTAS rebuild.
  - --skip-llm flag skips LLM fallback.

Usage:
    python scripts/421_canonical_us_nodule_tirads_ata_v1.py [--dry-run] [--skip-llm]

Author: Cursor Agent (Phase C.2), 2026-05-08
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"
LOCATION = "us-central1"

TABLE_MULTISYS = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_tirads_multisystem_v1"
TABLE_NODULE_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_v2"
TABLE_LN_CTX = f"{PROJECT}.{DATASET_WS}.us_nodule_ln_context_v1"
TABLE_SNAPSHOT = f"{PROJECT}.{DATASET_WS}.cpm_pre_tirads_multisystem_phaseC2_ata_snapshot_v1"
TABLE_STAGING = f"{PROJECT}.{DATASET_WS}.tirads_ata_scored_v1"
TABLE_FALLBACK_IN = f"{PROJECT}.{DATASET_WS}.tirads_ata_fallback_input_v1"
TABLE_FALLBACK_OUT = f"{PROJECT}.{DATASET_WS}.tirads_ata_fallback_output_v1"

PRO_MODEL = f"`{PROJECT}.{DATASET_WS}.gemini_25_pro`"
PIPELINE_VERSION = "phase_c2_ata_v1"
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
# Deterministic ATA 2015 scorer
# ---------------------------------------------------------------------------

MICRO_CALC_TOKENS = {"punctate_echogenic_foci", "punctate", "microcalcifications"}
SOLID_COMPOSITIONS = {"solid", "predominantly_solid", "almost_completely_solid"}
CYSTIC_MIXED = {"mixed_cystic_solid", "predominantly_cystic", "mixed", "partially_cystic"}
PURELY_CYSTIC = {"cystic", "almost_completely_cystic", "purely_cystic", "anechoic_cyst"}
HIGH_RISK_MARGINS = {"irregular", "microlobulated", "lobulated", "infiltrative", "spiculated"}


def _has_microcalc(echogenic_foci: Optional[str]) -> bool:
    if not echogenic_foci:
        return False
    try:
        items = json.loads(echogenic_foci)
        if isinstance(items, list):
            return any(str(i).lower() in MICRO_CALC_TOKENS for i in items)
        return str(items).lower() in MICRO_CALC_TOKENS
    except (json.JSONDecodeError, TypeError):
        lc = str(echogenic_foci).lower()
        return any(t in lc for t in MICRO_CALC_TOKENS)


def _has_rim_calc(echogenic_foci: Optional[str]) -> bool:
    """Check for peripheral rim / disrupted-rim calcifications."""
    if not echogenic_foci:
        return False
    try:
        items = json.loads(echogenic_foci)
        tokens = {str(i).lower() for i in (items if isinstance(items, list) else [items])}
    except (json.JSONDecodeError, TypeError):
        tokens = {str(echogenic_foci).lower()}
    return any(
        "rim" in t or "peripheral" in t or "eggshell" in t
        for t in tokens
    )


def _ata_high_risk_features(row: dict) -> list[str]:
    """ATA high-risk features for upgrading to 'high' pattern."""
    hrf = []
    margins = (row.get("margins") or "").lower()
    shape = (row.get("shape") or "").lower()
    echogenic_foci = row.get("echogenic_foci")
    ete_presence = (row.get("ete_on_us_presence_simple") or "").lower()

    if margins in HIGH_RISK_MARGINS:
        hrf.append(f"irregular_margins:{margins}")
    if shape == "taller_than_wide":
        hrf.append("taller_than_wide")
    if _has_microcalc(echogenic_foci):
        hrf.append("microcalcifications")
    if _has_rim_calc(echogenic_foci):
        hrf.append("rim_calcification")
    if ete_presence not in ("", "none", "unstated", "absent"):
        hrf.append(f"ete_on_us:{ete_presence}")
    return hrf


def score_ata(row: dict) -> dict:
    """Apply ATA 2015 decision tree to a single nodule row."""
    composition = (row.get("composition") or "").lower()
    echogenicity = (row.get("echogenicity") or "").lower()
    shape = (row.get("shape") or "").lower()
    margins = (row.get("margins") or "").lower()
    size_cm = row.get("size_cm_max")
    has_suspicious_ln = row.get("has_suspicious_ln_within_60d") or False

    has_composition = bool(composition)
    has_echo = bool(echogenicity)
    primitives_sufficient = has_composition and has_echo

    pattern = None
    high_risk_features = []
    decision_method = "deterministic"

    # Rule 1: Purely cystic, no solid component → benign
    if composition in PURELY_CYSTIC:
        pattern = "benign"

    # Rule 2: Spongiform → very_low
    if pattern is None and composition == "spongiform":
        pattern = "very_low"

    # Rule 3 & 4: High suspicion — hypoechoic + ≥1 HRF
    if pattern is None:
        is_hypo = echogenicity in ("hypoechoic", "very_hypoechoic", "markedly_hypoechoic",
                                    "slightly_hypoechoic", "mildly_hypoechoic")
        hrf = _ata_high_risk_features(row)

        if is_hypo and hrf:
            if (composition in SOLID_COMPOSITIONS
                    or (composition in CYSTIC_MIXED)):
                pattern = "high"
                high_risk_features = hrf

    # Rule 5: Intermediate — solid hypoechoic, smooth, no HRF
    if pattern is None and has_echo:
        is_hypo_simple = echogenicity in ("hypoechoic", "very_hypoechoic",
                                           "slightly_hypoechoic", "mildly_hypoechoic")
        if (composition in SOLID_COMPOSITIONS
                and is_hypo_simple
                and margins in ("smooth", "well_defined", "")
                and not _has_microcalc(row.get("echogenic_foci"))
                and shape != "taller_than_wide"
                and (row.get("ete_on_us_presence_simple") or "").lower()
                    in ("", "none", "unstated", "absent")):
            pattern = "intermediate"

    # Rule 6: Low — solid iso/hyperechoic, no HRF
    if pattern is None and has_echo:
        if (composition in SOLID_COMPOSITIONS
                and echogenicity in ("isoechoic", "hyperechoic", "iso", "hyper")
                and not _ata_high_risk_features(row)):
            pattern = "low"

    # Rule 7: Low — mixed/predominantly_cystic, no suspicious features
    if pattern is None and composition in CYSTIC_MIXED:
        if not _ata_high_risk_features(row):
            pattern = "low"

    # Rule 8: Very_low — mixed/predominantly_cystic with no pattern yet
    if pattern is None and composition in CYSTIC_MIXED:
        pattern = "very_low"

    # FNA recommendations per ATA 2015
    fna_recommended = False
    if pattern and size_cm is not None:
        if pattern == "high" and (size_cm >= 1.0 or has_suspicious_ln):
            fna_recommended = True
        elif pattern == "intermediate" and size_cm >= 1.0:
            fna_recommended = True
        elif pattern == "low" and size_cm >= 1.5:
            fna_recommended = True
        elif pattern == "very_low" and size_cm >= 2.0:
            fna_recommended = True
    # LN modifier: FNA for subcentimeter high/intermediate if LN suspicious
    if has_suspicious_ln and pattern in ("high", "intermediate") and size_cm is not None and size_cm < 1.0:
        fna_recommended = True

    needs_llm = (pattern is None) and primitives_sufficient

    return {
        "pattern": pattern,
        "high_risk_features_json": json.dumps(high_risk_features) if high_risk_features else None,
        "suspicious_ln_at_exam": bool(has_suspicious_ln),
        "decision_method": decision_method if pattern is not None else None,
        "fna_recommended": fna_recommended if pattern else None,
        "needs_llm_fallback": needs_llm,
        "primitives_sufficient": primitives_sufficient,
    }


# ---------------------------------------------------------------------------
# LLM fallback
# ---------------------------------------------------------------------------

ATA_FALLBACK_OUTPUT_SCHEMA = """\
pattern STRING,
high_risk_features_json STRING,
evidence_short STRING,
confidence FLOAT64"""


def build_fallback_input_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE `{TABLE_FALLBACK_IN}`
CLUSTER BY research_id AS
SELECT
  s.nodule_id,
  s.research_id,
  s.us_exam_id,
  s.exam_date,
  CONCAT(
    '[ATA 2015 assignment] Features: ',
    'composition=', COALESCE(v.composition, 'unknown'), '; ',
    'echogenicity=', COALESCE(v.echogenicity, 'unknown'), '; ',
    'shape=', COALESCE(v.shape, 'unknown'), '; ',
    'margins=', COALESCE(v.margins, 'unknown'), '; ',
    'echogenic_foci=', COALESCE(v.echogenic_foci, 'unknown'), '; ',
    'ete_us=', COALESCE(v.ete_on_us_presence_simple, 'unstated'), '; ',
    'size_cm=', COALESCE(CAST(v.size_cm_max AS STRING), 'unknown'), '. ',
    'Assign ATA 2015 pattern: benign | very_low | low | intermediate | high. ',
    'benign=purely cystic; very_low=spongiform or mixed no features; ',
    'low=solid iso/hyper or mixed cystic no HRF; ',
    'intermediate=solid hypo smooth no HRF; ',
    'high=hypo solid/cystic with irregular margins/microcalc/TTW/ETE. ',
    'evidence_short<=140 chars no PHI.'
  ) AS prompt
FROM `{TABLE_STAGING}` s
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
    '{ATA_FALLBACK_OUTPUT_SCHEMA}' AS output_schema,
    0.0 AS temperature,
    1024 AS max_output_tokens
  )
);
"""


def build_ctas_sql(skip_llm: bool) -> str:
    if skip_llm:
        llm_join = ""
        llm_pattern = "CAST(NULL AS STRING)"
        llm_hrf = "CAST(NULL AS STRING)"
        llm_method = "CAST(NULL AS STRING)"
        llm_fna = "CAST(NULL AS BOOL)"
    else:
        llm_join = f"\n  LEFT JOIN `{TABLE_FALLBACK_OUT}` llm ON m.nodule_id = llm.nodule_id"
        llm_pattern = "llm.pattern"
        llm_hrf = "llm.high_risk_features_json"
        llm_method = "CASE WHEN llm.pattern IS NOT NULL THEN 'llm_gemini_25_pro' ELSE NULL END"
        llm_fna = (
            "CASE "
            "  WHEN llm.pattern = 'high' AND m.size_cm_max >= 1.0 THEN TRUE "
            "  WHEN llm.pattern = 'intermediate' AND m.size_cm_max >= 1.0 THEN TRUE "
            "  WHEN llm.pattern = 'low' AND m.size_cm_max >= 1.5 THEN TRUE "
            "  WHEN llm.pattern = 'very_low' AND m.size_cm_max >= 2.0 THEN TRUE "
            "  WHEN llm.pattern IS NOT NULL THEN FALSE "
            "  ELSE NULL END"
        )

    return f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id AS
SELECT
  m.* EXCEPT (
    ata_pattern, ata_high_risk_features_json,
    ata_suspicious_ln_at_exam, ata_decision_method, ata_fna_recommended
  ),
  COALESCE(s.pattern, {llm_pattern}) AS ata_pattern,
  COALESCE(s.high_risk_features_json, {llm_hrf}) AS ata_high_risk_features_json,
  s.suspicious_ln_at_exam AS ata_suspicious_ln_at_exam,
  COALESCE(
    s.decision_method,
    {llm_method}
  ) AS ata_decision_method,
  COALESCE(s.fna_recommended, {llm_fna}) AS ata_fna_recommended
FROM `{TABLE_MULTISYS}` m
LEFT JOIN `{TABLE_STAGING}` s ON m.nodule_id = s.nodule_id{llm_join};
"""


AUDIT_SQL = f"""
SELECT
  ata_pattern,
  ata_decision_method,
  COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct
FROM `{TABLE_MULTISYS}`
GROUP BY 1, 2
ORDER BY 1, 2;
"""

SANITY_SQL = f"""
SELECT
  COUNTIF(ata_pattern IS NOT NULL) AS n_scored,
  COUNTIF(ata_pattern = 'benign') AS n_benign,
  COUNTIF(ata_pattern = 'very_low') AS n_very_low,
  COUNTIF(ata_pattern = 'low') AS n_low,
  COUNTIF(ata_pattern = 'intermediate') AS n_intermediate,
  COUNTIF(ata_pattern = 'high') AS n_high,
  COUNTIF(ata_decision_method = 'llm_gemini_25_pro') AS n_llm,
  COUNT(*) AS n_total
FROM `{TABLE_MULTISYS}`;
"""


def run_audit(bq: bigquery.Client) -> dict:
    _log("Audit: ATA distribution")
    for r in bq.query(AUDIT_SQL, location=LOCATION).result():
        _log(f"  {dict(r)}")

    sanity = dict(next(iter(bq.query(SANITY_SQL, location=LOCATION).result())))
    _log(f"Audit sanity: {sanity}")
    n_scored = sanity["n_scored"]
    if n_scored == 0:
        _halt("Audit: 0 scored rows")

    for cat, key in [("high", "n_high"), ("low", "n_low"), ("intermediate", "n_intermediate")]:
        n_cat = sanity[key]
        if n_scored and n_cat / n_scored > 0.70:
            _halt(f"Audit: {cat} dominates at {n_cat/n_scored:.1%} > 70%")

    n_llm = sanity["n_llm"]
    rate = n_llm / max(1, n_scored)
    _log(f"  LLM fallback rate: {rate:.1%} {'✓' if rate <= 0.25 else 'WARNING > 25%'}")
    return sanity


def append_dfl_row(bq: bigquery.Client, dry_run: bool, audit: dict) -> None:
    if dry_run:
        _log("DFL: skipped (dry-run)")
        return
    try:
        row = {
            "logged_at": datetime.now(timezone.utc).isoformat(),
            "target_type": "BQ infrastructure",
            "change_type": "new_column_data",
            "target_table": TABLE_MULTISYS,
            "target_column": "ata_*",
            "action_summary": (
                f"Phase C.2 ATA 2015 scorer applied. "
                f"n_scored={audit.get('n_scored')}, n_high={audit.get('n_high')}, "
                f"n_llm={audit.get('n_llm')}. Pipeline={PIPELINE_VERSION}."
            )[:280],
            "lifecycle": "Applied",
            "source_chat": "Phase C.2 ATA 2015 cursor prompt 2026-05-08",
            "phi_guard_confirmed": True,
        }
        errors = bq.insert_rows_json(
            f"{PROJECT}.pub_signoff.data_feedback_log_v1", [row]
        )
        if errors:
            _log(f"DFL WARNING: {errors}")
        else:
            _log("DFL: row inserted (lifecycle=Applied)")
    except Exception as e:
        _log(f"DFL: failed ({e}). Continuing.")


def main() -> None:
    parser = argparse.ArgumentParser(description="ATA 2015 scorer (Phase C.2)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-llm", action="store_true")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    bq = bigquery.Client(project=args.project)

    # Step 1: Snapshot
    _log("Step 1: Snapshot multisystem table")
    _run_sql(bq, f"CREATE OR REPLACE TABLE `{TABLE_SNAPSHOT}` AS SELECT * FROM `{TABLE_MULTISYS}`",
             "Snapshot")
    n_snap = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_SNAPSHOT}`")
    _log(f"  Snapshot rows: {n_snap}")

    # Step 2: Pull from v2 + LN context
    _log("Step 2: Pull nodule data + LN context")
    pull_sql = f"""
    SELECT
      n.nodule_id, n.research_id, n.us_exam_id, n.exam_date,
      n.composition, n.echogenicity, n.shape, n.margins,
      n.echogenic_foci, n.ete_on_us_presence_simple, n.size_cm_max,
      n.halo_presence_simple, n.vascularity_distribution_simple,
      COALESCE(CAST(ln.has_suspicious_ln_within_60d AS BOOL), FALSE) AS has_suspicious_ln_within_60d
    FROM `{TABLE_NODULE_V2}` n
    LEFT JOIN `{TABLE_LN_CTX}` ln USING (nodule_id)
    """
    rows = list(bq.query(pull_sql, location=LOCATION).result())
    _log(f"  Pulled {len(rows)} rows")

    # Step 3: Score
    _log("Step 3: Apply ATA 2015 decision tree")
    scored = []
    n_fallback = 0
    for row in rows:
        rd = dict(row)
        result = score_ata(rd)
        scored.append({
            "nodule_id": rd["nodule_id"],
            "research_id": rd["research_id"],
            "pattern": result["pattern"],
            "high_risk_features_json": result["high_risk_features_json"],
            "suspicious_ln_at_exam": result["suspicious_ln_at_exam"],
            "decision_method": result["decision_method"],
            "fna_recommended": result["fna_recommended"],
            "needs_llm_fallback": result["needs_llm_fallback"],
        })
        if result["needs_llm_fallback"]:
            n_fallback += 1

    n_det = sum(1 for s in scored if s["pattern"] is not None)
    _log(f"  Deterministic: {n_det}, LLM fallback: {n_fallback}")

    # Step 4: Write staging
    _log("Step 4: Write ATA staging table")
    import pandas as pd
    df_staged = pd.DataFrame(scored)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    load_job = bq.load_table_from_dataframe(df_staged, TABLE_STAGING,
                                             job_config=job_config, location=LOCATION)
    load_job.result()
    n_staged = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_STAGING}`")
    _log(f"  Staging: {n_staged} rows")

    if args.dry_run:
        _log("DRY RUN: stopping before LLM + CTAS.")
        return

    # Step 5: LLM fallback
    skip_llm = args.skip_llm or n_fallback == 0
    if not skip_llm:
        _log(f"Step 5: LLM fallback ({n_fallback} rows)")
        _run_sql(bq, build_fallback_input_sql(), "Build ATA fallback input")
        _run_sql(bq, LLM_FALLBACK_SQL, "ATA LLM fallback full run")
        n_out = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_FALLBACK_OUT}`")
        _log(f"  LLM output: {n_out} rows")
    else:
        _log("Step 5: Skipping LLM fallback")

    # Step 6: CTAS rebuild
    _log("Step 6: CTAS rebuild")
    _run_sql(bq, build_ctas_sql(skip_llm), "ATA CTAS rebuild")
    n_rebuilt = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_MULTISYS}`")
    if n_rebuilt != n_snap:
        _halt(f"Row count mismatch: snap={n_snap}, rebuilt={n_rebuilt}")

    # Step 7: Audit + DFL
    audit = run_audit(bq)
    append_dfl_row(bq, args.dry_run, audit)
    _log(f"Phase C.2 ATA complete. n_scored={audit.get('n_scored')}")


if __name__ == "__main__":
    main()
