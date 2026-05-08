"""
Script 430 — Build qc_tirads_multisystem_disagreement_v1
=========================================================
Step 5 of CURSOR_PROMPT_PHASE_E_PATCH_AND_RESUME_20260507.md.

Builds the multi-system disagreement queue per the exact SQL in that prompt.
Prerequisites:
  - canonical_us_nodule_tirads_multisystem_v1 with all 11 systems populated (Steps 1–4)

Verifies:
  - Queue size in 1,500–5,000 rows range (warns outside this range; does not halt)
  - Per-system suspicion ordinal distribution sanity

Output:
  pub_workspace.qc_tirads_multisystem_disagreement_v1
"""

import argparse
import sys
from datetime import datetime, timezone
from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"
LOCATION = "us-central1"

TABLE_MULTISYS = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_tirads_multisystem_v1"
TABLE_DISAGQ = f"{PROJECT}.{DATASET_WS}.qc_tirads_multisystem_disagreement_v1"


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def _run_sql(bq: bigquery.Client, sql: str, label: str) -> bigquery.QueryJob:
    job = bq.query(sql, location=LOCATION)
    job.result()
    _log(f"  ✓ {label} (job_id={job.job_id})")
    return job


def _scalar(bq: bigquery.Client, sql: str):
    return list(bq.query(sql, location=LOCATION).result())[0][0]


# Exact SQL from CURSOR_PROMPT_PHASE_E_PATCH_AND_RESUME_20260507.md Step 5
BUILD_QUEUE_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_DISAGQ}`
CLUSTER BY research_id AS
WITH normalized AS (
  SELECT
    nodule_id, research_id, us_exam_id, exam_date,
    CASE acr2017_category_imputed
      WHEN 'TR1' THEN 1 WHEN 'TR2' THEN 2 WHEN 'TR3' THEN 3
      WHEN 'TR4' THEN 4 WHEN 'TR5' THEN 5 END AS sus_acr,
    CASE kwak_category
      WHEN '2' THEN 1 WHEN '3' THEN 2 WHEN '4A' THEN 3
      WHEN '4B' THEN 4 WHEN '4C' THEN 5 WHEN '5' THEN 5 END AS sus_kwak,
    CASE ktirads_category
      WHEN '1' THEN 1 WHEN '2' THEN 1 WHEN '3' THEN 2
      WHEN '4' THEN 4 WHEN '5' THEN 5 END AS sus_ktirads,
    CASE ctirads_category
      WHEN '2' THEN 1 WHEN '3' THEN 2 WHEN '4A' THEN 3
      WHEN '4B' THEN 4 WHEN '4C' THEN 5 WHEN '5' THEN 5 WHEN '6' THEN 5 END AS sus_ctirads,
    CASE eutirads_category
      WHEN 'EU2' THEN 1 WHEN 'EU3' THEN 2 WHEN 'EU4' THEN 3 WHEN 'EU5' THEN 5 END AS sus_eu,
    CASE ata_pattern
      WHEN 'benign' THEN 1 WHEN 'very_low' THEN 1 WHEN 'low' THEN 2
      WHEN 'intermediate' THEN 3 WHEN 'high' THEN 5 END AS sus_ata,
    CASE bta_category
      WHEN 'U2' THEN 1 WHEN 'U3' THEN 2 WHEN 'U4' THEN 4 WHEN 'U5' THEN 5 END AS sus_bta,
    CASE aace_class
      WHEN 1 THEN 1 WHEN 2 THEN 3 WHEN 3 THEN 5 END AS sus_aace,
    CASE park2009_category
      WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3
      WHEN 'P4' THEN 4 WHEN 'P5' THEN 5 END AS sus_park2009,
    CASE park_cohort_category
      WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3
      WHEN 'P4' THEN 4 WHEN 'P5' THEN 5 END AS sus_park_cohort,
    CASE horvath_category
      WHEN '2' THEN 1 WHEN 'TIRADS_2' THEN 1
      WHEN '3' THEN 2 WHEN 'TIRADS_3' THEN 2
      WHEN '4A' THEN 3 WHEN 'TIRADS_4A' THEN 3
      WHEN '4B' THEN 4 WHEN 'TIRADS_4B' THEN 4
      WHEN '4C' THEN 5 WHEN 'TIRADS_4C' THEN 5
      WHEN '5' THEN 5 WHEN 'TIRADS_5' THEN 5 END AS sus_horvath
  FROM `{TABLE_MULTISYS}`
),
spreads AS (
  SELECT *,
    GREATEST(IFNULL(sus_acr,0), IFNULL(sus_kwak,0), IFNULL(sus_ktirads,0),
             IFNULL(sus_ctirads,0), IFNULL(sus_eu,0), IFNULL(sus_ata,0),
             IFNULL(sus_bta,0), IFNULL(sus_aace,0), IFNULL(sus_park2009,0),
             IFNULL(sus_park_cohort,0), IFNULL(sus_horvath,0)) AS max_sus,
    LEAST(IFNULL(sus_acr,9), IFNULL(sus_kwak,9), IFNULL(sus_ktirads,9),
          IFNULL(sus_ctirads,9), IFNULL(sus_eu,9), IFNULL(sus_ata,9),
          IFNULL(sus_bta,9), IFNULL(sus_aace,9), IFNULL(sus_park2009,9),
          IFNULL(sus_park_cohort,9), IFNULL(sus_horvath,9)) AS min_sus,
    (CASE WHEN sus_acr        IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN sus_kwak     IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN sus_ktirads  IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN sus_ctirads  IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN sus_eu       IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN sus_ata      IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN sus_bta      IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN sus_aace     IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN sus_park2009 IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN sus_park_cohort IS NOT NULL THEN 1 ELSE 0 END
     + CASE WHEN sus_horvath  IS NOT NULL THEN 1 ELSE 0 END) AS n_systems_scored
  FROM normalized
)
SELECT
  *,
  (max_sus - min_sus) AS suspicion_spread,
  CAST(NULL AS STRING) AS adjudication_status
FROM spreads
WHERE n_systems_scored >= 8
  AND (max_sus - min_sus) >= 2;
"""

# Audit: per-system coverage in normalized CTE
COVERAGE_SQL = f"""
SELECT
  COUNTIF(acr2017_category_imputed IS NOT NULL) AS n_acr,
  COUNTIF(kwak_category IS NOT NULL) AS n_kwak,
  COUNTIF(ktirads_category IS NOT NULL) AS n_ktirads,
  COUNTIF(ctirads_category IS NOT NULL) AS n_ctirads,
  COUNTIF(eutirads_category IS NOT NULL) AS n_eu,
  COUNTIF(ata_pattern IS NOT NULL) AS n_ata,
  COUNTIF(bta_category IS NOT NULL) AS n_bta,
  COUNTIF(aace_class IS NOT NULL) AS n_aace,
  COUNTIF(park2009_category IS NOT NULL) AS n_park2009,
  COUNTIF(park_cohort_category IS NOT NULL) AS n_park_cohort,
  COUNTIF(horvath_category IS NOT NULL) AS n_horvath,
  COUNT(*) AS n_total
FROM `{TABLE_MULTISYS}`
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Build TIRADS disagreement queue (Step 5)")
    parser.add_argument("--dry-run", action="store_true", help="Check coverage only; don't write queue")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    bq = bigquery.Client(project=args.project)

    _log("Coverage audit — all 11 systems")
    cov = dict(next(iter(bq.query(COVERAGE_SQL, location=LOCATION).result())))
    n_total = cov["n_total"]
    for system, n in sorted(cov.items(), key=lambda x: -x[1]):
        if system == "n_total":
            continue
        pct = n / max(1, n_total)
        _log(f"  {system}: {n}/{n_total} ({pct:.1%})")

    # Verify Horvath is populated
    n_horvath = cov.get("n_horvath", 0)
    if n_horvath == 0:
        _log("HALT: horvath_category is NULL for all rows — run Step 4 (script 425) first.")
        sys.exit(2)
    _log(f"  Horvath coverage: {n_horvath}/{n_total} ({n_horvath/max(1,n_total):.1%}) ✓")

    if args.dry_run:
        _log("--dry-run: coverage check complete; skipping queue creation.")
        return

    _log("Building disagreement queue")
    _run_sql(bq, BUILD_QUEUE_SQL, "qc_tirads_multisystem_disagreement_v1")
    n_queue = int(_scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_DISAGQ}`"))
    _log(f"  Queue size: {n_queue} rows")

    # Sanity check
    if n_queue < 1500:
        _log(f"  WARNING: queue size {n_queue} < 1,500 — systems agree more than expected "
             "(may be good), or suspicion mapping has an issue. Verify CASE statements.")
    elif n_queue > 5000:
        _log(f"  WARNING: queue size {n_queue} > 5,000 — review suspicion mapping CASE "
             "statements; may indicate an off-by-one in ordinal scale definitions.")
    else:
        _log(f"  Queue size {n_queue}: within expected 1,500–5,000 range ✓")

    # Spread distribution
    spread_q = f"""
    SELECT suspicion_spread, COUNT(*) AS n
    FROM `{TABLE_DISAGQ}`
    GROUP BY 1 ORDER BY 1
    """
    _log("  Spread distribution:")
    for r in bq.query(spread_q, location=LOCATION).result():
        _log(f"    spread={r[0]}: {r[1]} rows")

    _log(f"Step 5 complete. Disagreement queue ready: {n_queue} rows. "
         f"Proceed to Step 6 (Phase E Sonnet audit).")

    return n_queue


if __name__ == "__main__":
    main()
