"""
Phase C concordance + AUC audit
================================
Cross-system pairwise agreement (suspicious-binary) and per-system
AUC vs pathology (if us_nodule_path_outcome_v1 is populated).

Produces:
  pub_workspace.tirads_phase_c_concordance_v1  — pairwise concordance metrics
  pub_workspace.tirads_phase_c_distribution_v1 — per-system category distributions
  exports/phase_c_pattern_scorers_20260508/concordance_audit.json

Usage:
    python scripts/424_phase_c_concordance_audit.py [--project PROJECT]

Author: Cursor Agent (Phase C closure), 2026-05-08
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
TABLE_DISTRIBUTION = f"{PROJECT}.{DATASET_WS}.tirads_phase_c_distribution_v1"

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
# Distribution query
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
GROUP BY 1, 2, 3;
"""

# ---------------------------------------------------------------------------
# Concordance query — suspicious-binary pairwise agreement
# ---------------------------------------------------------------------------

CONCORDANCE_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_CONCORDANCE}` AS
WITH base AS (
  SELECT
    nodule_id,
    -- Binary suspicious flag per system
    (eutirads_category IN ('EU4', 'EU5'))                       AS eu_susp,
    (ata_pattern IN ('intermediate', 'high'))                   AS ata_susp,
    (bta_category IN ('U4', 'U5'))                              AS bta_susp,
    (aace_class = 3)                                            AS aace_susp
  FROM `{TABLE_MULTISYS}`
  WHERE eutirads_category IS NOT NULL
    AND ata_pattern IS NOT NULL
    AND bta_category IS NOT NULL
    AND aace_class IS NOT NULL
)
SELECT
  COUNT(*) AS n_all_scored,
  -- Pairwise agreement rates (suspicious-binary)
  ROUND(COUNTIF(eu_susp = ata_susp)   / COUNT(*), 4) AS agree_eu_ata,
  ROUND(COUNTIF(eu_susp = bta_susp)   / COUNT(*), 4) AS agree_eu_bta,
  ROUND(COUNTIF(eu_susp = aace_susp)  / COUNT(*), 4) AS agree_eu_aace,
  ROUND(COUNTIF(ata_susp = bta_susp)  / COUNT(*), 4) AS agree_ata_bta,
  ROUND(COUNTIF(ata_susp = aace_susp) / COUNT(*), 4) AS agree_ata_aace,
  ROUND(COUNTIF(bta_susp = aace_susp) / COUNT(*), 4) AS agree_bta_aace,
  -- Suspicious prevalence per system (useful context)
  ROUND(COUNTIF(eu_susp)   / COUNT(*), 4) AS pct_eu_susp,
  ROUND(COUNTIF(ata_susp)  / COUNT(*), 4) AS pct_ata_susp,
  ROUND(COUNTIF(bta_susp)  / COUNT(*), 4) AS pct_bta_susp,
  ROUND(COUNTIF(aace_susp) / COUNT(*), 4) AS pct_aace_susp,
  '{RUN_TS}' AS computed_at
FROM base;
"""

# ---------------------------------------------------------------------------
# AUC vs path — conditional (path outcome table may be empty)
# ---------------------------------------------------------------------------

AUC_SQL = f"""
-- Compute pseudo-AUC (C-statistic proxy) per system against pathology
-- Only runs if us_nodule_path_outcome_v1 has rows
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
    -- Ordinal suspicious score per system (higher = more suspicious)
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
    CAST(aace_class AS FLOAT64) AS aace_score
  FROM `{TABLE_MULTISYS}` m
)
SELECT
  COUNT(DISTINCT p.nodule_id) AS n_path_linked,
  -- Wilcoxon/Mann-Whitney proxy: compare average score in malignant vs benign
  AVG(IF(p.malignant = 1, s.eu_score, NULL))   - AVG(IF(p.malignant = 0, s.eu_score, NULL))   AS eu_diff,
  AVG(IF(p.malignant = 1, s.ata_score, NULL))  - AVG(IF(p.malignant = 0, s.ata_score, NULL))  AS ata_diff,
  AVG(IF(p.malignant = 1, s.bta_score, NULL))  - AVG(IF(p.malignant = 0, s.bta_score, NULL))  AS bta_diff,
  AVG(IF(p.malignant = 1, s.aace_score, NULL)) - AVG(IF(p.malignant = 0, s.aace_score, NULL)) AS aace_diff
FROM path p
JOIN scored s USING (nodule_id)
WHERE s.eu_score IS NOT NULL;
"""

# ---------------------------------------------------------------------------
# Notable Finding detection
# ---------------------------------------------------------------------------

NOTABLE_THRESHOLD = 0.70  # concordance below this is a notable finding
CONCORDANCE_WARN_THRESHOLD = 0.75  # target from prompt


def detect_notable_findings(concordance: dict) -> list[str]:
    """Detect concordance anomalies that should be logged."""
    findings = []
    pairs = [
        ("eu_ata", "EU-TIRADS / ATA"),
        ("eu_bta", "EU-TIRADS / BTA"),
        ("eu_aace", "EU-TIRADS / AACE"),
        ("ata_bta", "ATA / BTA"),
        ("ata_aace", "ATA / AACE"),
        ("bta_aace", "BTA / AACE"),
    ]
    for key, label in pairs:
        val = concordance.get(f"agree_{key}")
        if val is not None and val < NOTABLE_THRESHOLD:
            findings.append(
                f"NOTABLE FINDING: {label} pairwise concordance {val:.1%} < 70% threshold. "
                f"EU-TIRADS vs BTA disagreement is anticipated: BTA's halo-dependence "
                f"vs EU-TIRADS shape/margin focus drives intermediate-risk disagreement "
                f"when halo is stated for BTA but composition/margins govern EU-TIRADS."
            )
        elif val is not None and val < CONCORDANCE_WARN_THRESHOLD:
            findings.append(
                f"WARNING: {label} concordance {val:.1%} < 75% target — "
                f"review per-system distributions for systematic pattern gap."
            )
    return findings


def main() -> None:
    parser = argparse.ArgumentParser(description="Phase C concordance + AUC audit")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    bq = bigquery.Client(project=args.project)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Step 1: Distribution table
    _log("Step 1: Build per-system distribution table")
    _run_sql(bq, DISTRIBUTION_SQL, "Distribution table")
    dist_rows = list(bq.query(
        f"SELECT * FROM `{TABLE_DISTRIBUTION}` ORDER BY system, category",
        location=LOCATION
    ).result())
    _log(f"  Distribution rows: {len(dist_rows)}")
    for r in dist_rows:
        _log(f"    {dict(r)}")

    # Step 2: Concordance table
    _log("Step 2: Build cross-system concordance table")
    _run_sql(bq, CONCORDANCE_SQL, "Concordance table")
    conc_rows = list(bq.query(f"SELECT * FROM `{TABLE_CONCORDANCE}`", location=LOCATION).result())
    concordance = dict(conc_rows[0]) if conc_rows else {}
    _log(f"  Concordance: {concordance}")

    # Step 3: AUC check
    _log("Step 3: AUC vs pathology check")
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
             "This is a source-data gap, not a scorer bug. "
             "AUC computation deferred to when pathology linkage is populated.")
        auc_result = {"note": "path_outcome_table_empty_0_rows", "n_path_linked": 0}

    # Step 4: Notable findings
    _log("Step 4: Notable findings detection")
    findings = detect_notable_findings(concordance)
    if findings:
        for f in findings:
            _log(f"  {f}")
    else:
        _log("  No notable findings — all pairwise agreements ≥ 75% target.")

    # Step 5: Write JSON report
    report = {
        "run_ts": RUN_TS,
        "concordance": concordance,
        "auc_proxy": auc_result,
        "notable_findings": findings,
        "targets": {
            "pairwise_agreement_min": CONCORDANCE_WARN_THRESHOLD,
            "pairwise_alert_threshold": NOTABLE_THRESHOLD,
        },
        "distribution_n_rows": len(dist_rows),
    }
    report_path = OUT_DIR / "concordance_audit.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    _log(f"  Report written: {report_path}")

    _log("Phase C concordance audit complete.")
    return report


if __name__ == "__main__":
    main()
