"""
Phase C concordance + AUC audit — updated for 5-way concordance (v2, Phase C.5)
================================================================================
Cross-system pairwise agreement (suspicious-binary) and per-system
AUC vs pathology (if us_nodule_path_outcome_v1 is populated).

Systems included (Phase B + C):
  1. ACR TI-RADS (Phase B) — acr_category / acr_tirads_score
  2. EU-TIRADS 2017 (Phase C.1) — eutirads_category
  3. ATA 2015 (Phase C.2) — ata_pattern
  4. BTA U1–U5 2014 (Phase C.3) — bta_category
  5. AACE 2016 (Phase C.4) — aace_class
  6. Horvath / Chilean 2009 (Phase C.5) — horvath_category  ← NEW

Also produces:
  pub_workspace.tirads_phase_c_concordance_v1          — 4-way concordance (C.1–C.4)
  pub_workspace.tirads_phase_c5_concordance_v1         — 5-way concordance (C.1–C.5)
  pub_workspace.tirads_phase_c_distribution_v1         — per-system distributions
  pub_workspace.qc_tirads_multisystem_disagreement_v1  — Phase E input queue
  exports/phase_c_pattern_scorers_20260508/concordance_audit.json

Usage:
    python scripts/424_phase_c_concordance_audit.py [--project PROJECT] [--include-horvath]

Author: Cursor Agent (Phase C closure + Phase C.5 closure), 2026-05-08
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"
LOCATION = "us-central1"

TABLE_MULTISYS = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_tirads_multisystem_v1"
TABLE_PATH = f"{PROJECT}.{DATASET_WS}.us_nodule_path_outcome_v1"
TABLE_CONCORDANCE = f"{PROJECT}.{DATASET_WS}.tirads_phase_c_concordance_v1"
TABLE_CONCORDANCE_5WAY = f"{PROJECT}.{DATASET_WS}.tirads_phase_c5_concordance_v1"
TABLE_DISTRIBUTION = f"{PROJECT}.{DATASET_WS}.tirads_phase_c_distribution_v1"
TABLE_DISAGREE = f"{PROJECT}.{DATASET_WS}.qc_tirads_multisystem_disagreement_v1"

OUT_DIR = Path(__file__).resolve().parent.parent / "exports" / "phase_c_pattern_scorers_20260508"

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


# ---------------------------------------------------------------------------
# Distribution query — all 6 systems
# ---------------------------------------------------------------------------

DISTRIBUTION_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_DISTRIBUTION}` AS

SELECT
  'eu_tirads' AS system,
  eutirads_category AS category,
  eutirads_decision_method AS decision_method,
  COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY 'eu_tirads'), 4) AS pct
FROM `{TABLE_MULTISYS}`
WHERE eutirads_category IS NOT NULL
GROUP BY 1, 2, 3

UNION ALL

SELECT
  'ata_2015' AS system,
  ata_pattern AS category,
  ata_decision_method AS decision_method,
  COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY 'ata_2015'), 4) AS pct
FROM `{TABLE_MULTISYS}`
WHERE ata_pattern IS NOT NULL
GROUP BY 1, 2, 3

UNION ALL

SELECT
  'bta_2014' AS system,
  bta_category AS category,
  bta_decision_method AS decision_method,
  COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY 'bta_2014'), 4) AS pct
FROM `{TABLE_MULTISYS}`
WHERE bta_category IS NOT NULL
GROUP BY 1, 2, 3

UNION ALL

SELECT
  'aace_2016' AS system,
  CAST(aace_class AS STRING) AS category,
  aace_decision_method AS decision_method,
  COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY 'aace_2016'), 4) AS pct
FROM `{TABLE_MULTISYS}`
WHERE aace_class IS NOT NULL
GROUP BY 1, 2, 3

UNION ALL

SELECT
  'horvath_2009' AS system,
  horvath_category AS category,
  horvath_decision_method AS decision_method,
  COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY 'horvath_2009'), 4) AS pct
FROM `{TABLE_MULTISYS}`
WHERE horvath_category IS NOT NULL
GROUP BY 1, 2, 3;
"""

# ---------------------------------------------------------------------------
# 4-way concordance (C.1–C.4, backward-compatible)
# ---------------------------------------------------------------------------

CONCORDANCE_4WAY_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_CONCORDANCE}` AS
WITH base AS (
  SELECT
    nodule_id,
    (eutirads_category IN ('EU4', 'EU5'))                       AS eu_susp,
    (ata_pattern IN ('intermediate', 'high'))                   AS ata_susp,
    (bta_category IN ('U4', 'U5'))                             AS bta_susp,
    (aace_class = 3)                                           AS aace_susp
  FROM `{TABLE_MULTISYS}`
  WHERE eutirads_category IS NOT NULL
    AND ata_pattern IS NOT NULL
    AND bta_category IS NOT NULL
    AND aace_class IS NOT NULL
)
SELECT
  COUNT(*) AS n_all_scored,
  ROUND(COUNTIF(eu_susp = ata_susp)   / COUNT(*), 4) AS agree_eu_ata,
  ROUND(COUNTIF(eu_susp = bta_susp)   / COUNT(*), 4) AS agree_eu_bta,
  ROUND(COUNTIF(eu_susp = aace_susp)  / COUNT(*), 4) AS agree_eu_aace,
  ROUND(COUNTIF(ata_susp = bta_susp)  / COUNT(*), 4) AS agree_ata_bta,
  ROUND(COUNTIF(ata_susp = aace_susp) / COUNT(*), 4) AS agree_ata_aace,
  ROUND(COUNTIF(bta_susp = aace_susp) / COUNT(*), 4) AS agree_bta_aace,
  ROUND(COUNTIF(eu_susp)   / COUNT(*), 4) AS pct_eu_susp,
  ROUND(COUNTIF(ata_susp)  / COUNT(*), 4) AS pct_ata_susp,
  ROUND(COUNTIF(bta_susp)  / COUNT(*), 4) AS pct_bta_susp,
  ROUND(COUNTIF(aace_susp) / COUNT(*), 4) AS pct_aace_susp,
  '{RUN_TS}' AS computed_at
FROM base;
"""

# ---------------------------------------------------------------------------
# 5-way concordance (C.1–C.5, including Horvath)
# ---------------------------------------------------------------------------

# Horvath suspicious-binary:
#   TIRADS_4A, TIRADS_4B, TIRADS_4C, TIRADS_5 → suspicious
#   TIRADS_2, TIRADS_3 → not suspicious

CONCORDANCE_5WAY_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_CONCORDANCE_5WAY}` AS
WITH base AS (
  SELECT
    nodule_id,
    research_id,
    (eutirads_category IN ('EU4', 'EU5'))                         AS eu_susp,
    (ata_pattern IN ('intermediate', 'high'))                     AS ata_susp,
    (bta_category IN ('U4', 'U5'))                               AS bta_susp,
    (aace_class = 3)                                             AS aace_susp,
    (horvath_category IN ('TIRADS_4A','TIRADS_4B','TIRADS_4C','TIRADS_5'))
                                                                  AS horvath_susp,
    -- Ordinal suspicion scores for disagreement distance
    CASE eutirads_category
      WHEN 'EU2' THEN 0 WHEN 'EU3' THEN 1 WHEN 'EU4' THEN 2 WHEN 'EU5' THEN 3
      ELSE NULL END AS eu_ord,
    CASE ata_pattern
      WHEN 'benign' THEN 0 WHEN 'very_low' THEN 1 WHEN 'low' THEN 2
      WHEN 'intermediate' THEN 3 WHEN 'high' THEN 4 ELSE NULL END AS ata_ord,
    CASE bta_category
      WHEN 'U2' THEN 0 WHEN 'U3' THEN 1 WHEN 'U4' THEN 2 WHEN 'U5' THEN 3
      ELSE NULL END AS bta_ord,
    CAST(aace_class AS FLOAT64) AS aace_ord,
    CASE horvath_category
      WHEN 'TIRADS_2' THEN 0 WHEN 'TIRADS_3' THEN 1
      WHEN 'TIRADS_4A' THEN 2 WHEN 'TIRADS_4B' THEN 3
      WHEN 'TIRADS_4C' THEN 4 WHEN 'TIRADS_5' THEN 5
      ELSE NULL END AS horvath_ord
  FROM `{TABLE_MULTISYS}`
  WHERE eutirads_category IS NOT NULL
    AND ata_pattern IS NOT NULL
    AND bta_category IS NOT NULL
    AND aace_class IS NOT NULL
    AND horvath_category IS NOT NULL
)
SELECT
  COUNT(*) AS n_all_5way_scored,
  -- Pairwise agreement rates (suspicious-binary)
  ROUND(COUNTIF(eu_susp = ata_susp)       / COUNT(*), 4) AS agree_eu_ata,
  ROUND(COUNTIF(eu_susp = bta_susp)       / COUNT(*), 4) AS agree_eu_bta,
  ROUND(COUNTIF(eu_susp = aace_susp)      / COUNT(*), 4) AS agree_eu_aace,
  ROUND(COUNTIF(eu_susp = horvath_susp)   / COUNT(*), 4) AS agree_eu_horvath,
  ROUND(COUNTIF(ata_susp = bta_susp)      / COUNT(*), 4) AS agree_ata_bta,
  ROUND(COUNTIF(ata_susp = aace_susp)     / COUNT(*), 4) AS agree_ata_aace,
  ROUND(COUNTIF(ata_susp = horvath_susp)  / COUNT(*), 4) AS agree_ata_horvath,
  ROUND(COUNTIF(bta_susp = aace_susp)     / COUNT(*), 4) AS agree_bta_aace,
  ROUND(COUNTIF(bta_susp = horvath_susp)  / COUNT(*), 4) AS agree_bta_horvath,
  ROUND(COUNTIF(aace_susp = horvath_susp) / COUNT(*), 4) AS agree_aace_horvath,
  -- Suspicious prevalence per system
  ROUND(COUNTIF(eu_susp)      / COUNT(*), 4) AS pct_eu_susp,
  ROUND(COUNTIF(ata_susp)     / COUNT(*), 4) AS pct_ata_susp,
  ROUND(COUNTIF(bta_susp)     / COUNT(*), 4) AS pct_bta_susp,
  ROUND(COUNTIF(aace_susp)    / COUNT(*), 4) AS pct_aace_susp,
  ROUND(COUNTIF(horvath_susp) / COUNT(*), 4) AS pct_horvath_susp,
  -- 5-way full agreement rate
  ROUND(COUNTIF(eu_susp = ata_susp
    AND ata_susp = bta_susp
    AND bta_susp = aace_susp
    AND aace_susp = horvath_susp) / COUNT(*), 4) AS agree_all_5way,
  '{RUN_TS}' AS computed_at
FROM base;
"""

# ---------------------------------------------------------------------------
# Phase E input queue — multi-system disagreement
# Per-nodule rows where max_ord - min_ord ≥ 2 (≥2 categories apart)
# ---------------------------------------------------------------------------

DISAGREE_QUEUE_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_DISAGREE}`
CLUSTER BY research_id AS
WITH scored AS (
  SELECT
    nodule_id,
    research_id,
    us_exam_id,
    exam_date,
    -- Ordinal scores (NULL if system didn't score)
    CASE eutirads_category
      WHEN 'EU2' THEN 0 WHEN 'EU3' THEN 1 WHEN 'EU4' THEN 2 WHEN 'EU5' THEN 3
      ELSE NULL END AS eu_ord,
    CASE ata_pattern
      WHEN 'benign' THEN 0 WHEN 'very_low' THEN 1 WHEN 'low' THEN 2
      WHEN 'intermediate' THEN 3 WHEN 'high' THEN 4 ELSE NULL END AS ata_ord,
    CASE bta_category
      WHEN 'U2' THEN 0 WHEN 'U3' THEN 1 WHEN 'U4' THEN 2 WHEN 'U5' THEN 3
      ELSE NULL END AS bta_ord,
    CAST(aace_class AS FLOAT64) AS aace_ord,
    CASE horvath_category
      WHEN 'TIRADS_2' THEN 0 WHEN 'TIRADS_3' THEN 1
      WHEN 'TIRADS_4A' THEN 2 WHEN 'TIRADS_4B' THEN 3
      WHEN 'TIRADS_4C' THEN 4 WHEN 'TIRADS_5' THEN 5
      ELSE NULL END AS horvath_ord,
    -- Raw assignments for review context
    eutirads_category,
    ata_pattern,
    bta_category,
    CAST(aace_class AS STRING) AS aace_class_str,
    horvath_category,
    horvath_pattern,
    -- Composition for stratification
    composition,
    size_cm_max
  FROM `{TABLE_MULTISYS}`
  WHERE eutirads_category IS NOT NULL
    AND ata_pattern IS NOT NULL
    AND bta_category IS NOT NULL
    AND aace_class IS NOT NULL
    AND horvath_category IS NOT NULL
),
with_range AS (
  SELECT
    *,
    GREATEST(
      COALESCE(eu_ord, 0), COALESCE(ata_ord, 0), COALESCE(bta_ord, 0),
      COALESCE(aace_ord, 0), COALESCE(horvath_ord, 0)
    ) AS max_ord,
    LEAST(
      COALESCE(eu_ord, 99), COALESCE(ata_ord, 99), COALESCE(bta_ord, 99),
      COALESCE(aace_ord, 99), COALESCE(horvath_ord, 99)
    ) AS min_ord
  FROM scored
)
SELECT
  nodule_id,
  research_id,
  us_exam_id,
  exam_date,
  -- Disagreement metrics
  (max_ord - min_ord)                    AS disagreement_distance,
  -- Which systems called suspicious vs not
  (eu_ord >= 2)                          AS eu_suspicious,
  (ata_ord >= 3)                         AS ata_suspicious,
  (bta_ord >= 2)                         AS bta_suspicious,
  (aace_ord >= 3)                        AS aace_suspicious,
  (horvath_ord >= 2)                     AS horvath_suspicious,
  -- System assignments
  eutirads_category,
  ata_pattern,
  bta_category,
  aace_class_str                         AS aace_class,
  horvath_category,
  horvath_pattern,
  -- Context
  composition,
  size_cm_max,
  -- Priority for Phase E adjudication
  CASE
    WHEN (max_ord - min_ord) >= 4 THEN 'critical'
    WHEN (max_ord - min_ord) = 3  THEN 'high'
    WHEN (max_ord - min_ord) = 2  THEN 'medium'
    ELSE 'low'
  END AS disagreement_priority,
  '{RUN_TS}'                             AS computed_at
FROM with_range
WHERE (max_ord - min_ord) >= 2
ORDER BY disagreement_distance DESC, nodule_id;
"""

# ---------------------------------------------------------------------------
# AUC vs path — 5-system
# ---------------------------------------------------------------------------

AUC_SQL = f"""
WITH path AS (
  SELECT
    nodule_id,
    CASE WHEN patient_has_any_mal_in_window THEN 1.0 ELSE 0.0 END AS malignant
  FROM `{TABLE_PATH}`
  WHERE patient_has_any_mal_in_window IS NOT NULL
),
scored AS (
  SELECT
    m.nodule_id,
    CASE eutirads_category
      WHEN 'EU2' THEN 0 WHEN 'EU3' THEN 1 WHEN 'EU4' THEN 2 WHEN 'EU5' THEN 3
      ELSE NULL END AS eu_score,
    CASE ata_pattern
      WHEN 'benign' THEN 0 WHEN 'very_low' THEN 1 WHEN 'low' THEN 2
      WHEN 'intermediate' THEN 3 WHEN 'high' THEN 4
      ELSE NULL END AS ata_score,
    CASE bta_category
      WHEN 'U2' THEN 0 WHEN 'U3' THEN 1 WHEN 'U4' THEN 2 WHEN 'U5' THEN 3
      ELSE NULL END AS bta_score,
    CAST(aace_class AS FLOAT64) AS aace_score,
    CASE horvath_category
      WHEN 'TIRADS_2' THEN 0 WHEN 'TIRADS_3' THEN 1
      WHEN 'TIRADS_4A' THEN 2 WHEN 'TIRADS_4B' THEN 3
      WHEN 'TIRADS_4C' THEN 4 WHEN 'TIRADS_5' THEN 5
      ELSE NULL END AS horvath_score
  FROM `{TABLE_MULTISYS}` m
)
SELECT
  COUNT(DISTINCT p.nodule_id) AS n_path_linked,
  -- Discriminative-difference proxy (malignant avg minus benign avg per system)
  AVG(IF(p.malignant = 1, s.eu_score, NULL))      - AVG(IF(p.malignant = 0, s.eu_score, NULL))
    AS eu_diff,
  AVG(IF(p.malignant = 1, s.ata_score, NULL))     - AVG(IF(p.malignant = 0, s.ata_score, NULL))
    AS ata_diff,
  AVG(IF(p.malignant = 1, s.bta_score, NULL))     - AVG(IF(p.malignant = 0, s.bta_score, NULL))
    AS bta_diff,
  AVG(IF(p.malignant = 1, s.aace_score, NULL))    - AVG(IF(p.malignant = 0, s.aace_score, NULL))
    AS aace_diff,
  AVG(IF(p.malignant = 1, s.horvath_score, NULL)) - AVG(IF(p.malignant = 0, s.horvath_score, NULL))
    AS horvath_diff
FROM path p
JOIN scored s USING (nodule_id)
WHERE s.eu_score IS NOT NULL;
"""

# ---------------------------------------------------------------------------
# Notable Finding detection
# ---------------------------------------------------------------------------

NOTABLE_THRESHOLD = 0.70
CONCORDANCE_WARN_THRESHOLD = 0.75


def detect_notable_findings(concordance_4: dict, concordance_5: dict) -> list[str]:
    findings = []

    all_pairs_4 = [
        ("agree_eu_ata", "EU-TIRADS / ATA"),
        ("agree_eu_bta", "EU-TIRADS / BTA"),
        ("agree_eu_aace", "EU-TIRADS / AACE"),
        ("agree_ata_bta", "ATA / BTA"),
        ("agree_ata_aace", "ATA / AACE"),
        ("agree_bta_aace", "BTA / AACE"),
    ]
    all_pairs_5 = [
        ("agree_eu_horvath", "EU-TIRADS / Horvath"),
        ("agree_ata_horvath", "ATA / Horvath"),
        ("agree_bta_horvath", "BTA / Horvath"),
        ("agree_aace_horvath", "AACE / Horvath"),
    ]
    all_pairs = [(concordance_4, all_pairs_4), (concordance_5, all_pairs_5)]

    for conc_dict, pairs in all_pairs:
        for key, label in pairs:
            val = conc_dict.get(key)
            if val is None:
                continue
            if val < NOTABLE_THRESHOLD:
                findings.append(
                    f"NOTABLE FINDING: {label} pairwise concordance {val:.1%} < 70% threshold. "
                    f"Systematic feature-specific disagreement — review per-pattern distributions."
                )
            elif val < CONCORDANCE_WARN_THRESHOLD:
                findings.append(
                    f"WARNING: {label} concordance {val:.1%} < 75% target. "
                    f"Review per-system distributions for pattern gap."
                )

    # Horvath Hashimoto pseudonodule finding (anticipated)
    pct_horv_susp = concordance_5.get("pct_horvath_susp")
    pct_eu_susp = concordance_5.get("pct_eu_susp")
    if pct_horv_susp is not None and pct_eu_susp is not None:
        if pct_horv_susp < pct_eu_susp - 0.10:
            findings.append(
                f"CANDIDATE FINDING: Horvath suspicious-rate ({pct_horv_susp:.1%}) is "
                f">10pp lower than EU-TIRADS ({pct_eu_susp:.1%}). "
                f"Likely driven by Horvath's Hashimoto/colloid pseudonodule patterns "
                f"reclassifying nodules that EU-TIRADS scores EU4/EU5."
            )

    agree_5 = concordance_5.get("agree_all_5way")
    if agree_5 is not None and agree_5 < 0.60:
        findings.append(
            f"NOTABLE FINDING: 5-way full agreement rate {agree_5:.1%} < 60%. "
            f"High system heterogeneity across all five scoring systems — "
            f"this cohort likely has a significant proportion of borderline nodules."
        )

    return findings


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase C concordance + AUC audit (5-way)")
    parser.add_argument("--project", default=PROJECT)
    parser.add_argument("--include-horvath", action="store_true", default=True,
                        help="Include Horvath (Phase C.5) in 5-way concordance (default: True).")
    args = parser.parse_args()

    bq = bigquery.Client(project=args.project)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Distribution table (all 5 systems)
    _log("Step 1: Build per-system distribution table (5 systems)")
    _run_sql(bq, DISTRIBUTION_SQL, "Distribution table")
    dist_rows = list(bq.query(
        f"SELECT * FROM `{TABLE_DISTRIBUTION}` ORDER BY system, category",
        location=LOCATION
    ).result())
    _log(f"  Distribution rows: {len(dist_rows)}")
    for r in dist_rows:
        _log(f"    {dict(r)}")

    # Step 2a: 4-way concordance (backward-compatible)
    _log("Step 2a: 4-way concordance (EU/ATA/BTA/AACE)")
    _run_sql(bq, CONCORDANCE_4WAY_SQL, "4-way concordance")
    conc4_rows = list(bq.query(f"SELECT * FROM `{TABLE_CONCORDANCE}`", location=LOCATION).result())
    concordance_4 = dict(conc4_rows[0]) if conc4_rows else {}
    _log(f"  4-way concordance: {concordance_4}")

    # Step 2b: 5-way concordance (including Horvath)
    _log("Step 2b: 5-way concordance (EU/ATA/BTA/AACE/Horvath)")

    # Check if Horvath columns exist and have data
    n_horvath = _scalar(
        bq,
        f"SELECT COUNTIF(horvath_category IS NOT NULL) FROM `{TABLE_MULTISYS}`"
    )
    concordance_5 = {}
    if n_horvath > 0:
        _run_sql(bq, CONCORDANCE_5WAY_SQL, "5-way concordance")
        conc5_rows = list(
            bq.query(f"SELECT * FROM `{TABLE_CONCORDANCE_5WAY}`", location=LOCATION).result()
        )
        concordance_5 = dict(conc5_rows[0]) if conc5_rows else {}
        _log(f"  5-way concordance: {concordance_5}")
    else:
        _log("  NOTE: Horvath columns not yet populated — 5-way concordance deferred.")
        concordance_5 = {"note": "horvath_not_yet_scored", "n_all_5way_scored": 0}

    # Step 3: Disagreement queue (Phase E input)
    _log("Step 3: Build multi-system disagreement queue")
    if n_horvath > 0:
        _run_sql(bq, DISAGREE_QUEUE_SQL, "qc_tirads_multisystem_disagreement_v1")
        n_disagree = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_DISAGREE}`")
        _log(f"  Disagreement queue: {n_disagree} nodules (≥2 category distance)")
        n_critical = _scalar(
            bq,
            f"SELECT COUNTIF(disagreement_priority='critical') FROM `{TABLE_DISAGREE}`"
        )
        n_high = _scalar(
            bq,
            f"SELECT COUNTIF(disagreement_priority='high') FROM `{TABLE_DISAGREE}`"
        )
        _log(f"  Priority breakdown: critical={n_critical}, high={n_high}")
    else:
        n_disagree = 0
        _log("  Disagreement queue deferred until Horvath is scored.")

    # Step 4: AUC check
    _log("Step 4: AUC vs pathology check")
    n_path = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_PATH}`")
    auc_result = {}
    if n_path > 0:
        _log(f"  Path table has {n_path} rows — computing discriminative-difference proxy")
        auc_rows = list(bq.query(AUC_SQL, location=LOCATION).result())
        if auc_rows:
            auc_result = dict(auc_rows[0])
            _log(f"  AUC proxy: {auc_result}")
    else:
        _log("  NOTE: us_nodule_path_outcome_v1 has 0 rows — AUC vs path not computable. "
             "Deferred to when pathology linkage is populated. This is a known source-data gap, "
             "not a scorer bug. AUC will be computed during Phase E (Opus adjudication).")
        auc_result = {"note": "path_outcome_table_empty_0_rows", "n_path_linked": 0}

    # Step 5: Notable findings
    _log("Step 5: Notable findings detection")
    findings = detect_notable_findings(concordance_4, concordance_5)
    if findings:
        for f in findings:
            _log(f"  {f}")
    else:
        _log("  No notable findings — all pairwise agreements ≥ 75% target.")

    # Step 6: Write JSON report
    report = {
        "run_ts": RUN_TS,
        "concordance_4way": concordance_4,
        "concordance_5way": concordance_5,
        "auc_proxy": auc_result,
        "notable_findings": findings,
        "disagreement_queue": {
            "n_total": n_disagree,
            "n_critical": n_critical if n_horvath > 0 else 0,
            "n_high": n_high if n_horvath > 0 else 0,
            "phase_e_ready": n_horvath > 0,
        },
        "targets": {
            "pairwise_agreement_min": CONCORDANCE_WARN_THRESHOLD,
            "pairwise_alert_threshold": NOTABLE_THRESHOLD,
            "5way_full_agreement_min": 0.60,
            "auc_target_per_system": 0.65,
        },
        "distribution_n_rows": len(dist_rows),
    }
    report_path = OUT_DIR / "concordance_audit_v2_5way.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    _log(f"  Report written: {report_path}")

    _log("Phase C concordance audit (5-way) complete.")
    return report


if __name__ == "__main__":
    main()
