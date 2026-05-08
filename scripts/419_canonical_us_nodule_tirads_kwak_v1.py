"""
Phase E patch — Step 3a — Kwak TI-RADS 2011 scorer
====================================================
Surfaces Kwak 2011 categories into
``pub_canonical.canonical_us_nodule_tirads_multisystem_v1`` per the verbatim
B.2 rules in ``CURSOR_PROMPT_PHASE_B_DETERMINISTIC_SCORERS_20260507.md``.

Five suspicious features (Kwak 2011):
  1. Solid composition (composition IN ('solid','predominantly_solid'))
  2. Hypoechogenicity (echogenicity IN ('hypoechoic','very_hypoechoic'))
  3. Irregular/microlobulated margins (margins IN ('irregular','lobulated','microlobulated'))
  4. Microcalcifications (echogenic_foci array contains 'punctate_echogenic_foci')
  5. Taller-than-wide shape (shape='taller_than_wide')

Category mapping:
  - TI-RADS 2 = composition='cystic' OR (composition='spongiform' AND n_susp=0)
  - TI-RADS 3 = n_susp=0 AND not in '2' path
  - TI-RADS 4A = n_susp=1
  - TI-RADS 4B = n_susp=2
  - TI-RADS 4C = n_susp IN (3, 4)
  - TI-RADS 5 = n_susp=5

FNA: 4A/4B/4C/5 AND size_cm_max >= 1.0

Audit gate: scorer-success-rate ≥98% on feasible rows
(feasible_kwak = rows with composition + echogenicity + shape + margins all
non-NULL; foci null-OK because microcalc absence is the relevant interpretation).

Author: Cursor Agent (Path A patch — Step 3a), 2026-05-08
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from google.cloud import bigquery

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.composition_normalize import normalize_composition_acr  # noqa: E402

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"
LOCATION = "us-central1"

TABLE_MULTISYS = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_tirads_multisystem_v1"
TABLE_NODULE_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_v2"
TABLE_SNAPSHOT = (
    f"{PROJECT}.{DATASET_WS}.cpm_pre_kwak_surface_to_multisystem_20260508_snapshot"
)
TABLE_STAGING = f"{PROJECT}.{DATASET_WS}.tirads_kwak_scored_v1"

PIPELINE_VERSION = "phase_e_patch_kwak_v1_20260508"


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def _halt(reason: str) -> None:
    _log(f"HALT: {reason}")
    sys.exit(2)


def _scalar(bq, sql):
    return next(iter(bq.query(sql, location=LOCATION).result()))[0]


def _run_sql(bq, sql, label):
    _log(f"SQL: {label}")
    job = bq.query(sql, location=LOCATION)
    job.result()
    _log(f"  ok job_id={job.job_id}")
    return job


def _coerce(v):
    if v is None:
        return None
    s = str(v).strip().lower()
    return s if s else None


def _has_microcalc(foci_raw: Optional[str]) -> Optional[bool]:
    """Return True if foci JSON-array contains 'punctate_echogenic_foci',
    False if foci is parseable and lacks it, None if foci is null/unparseable.
    """
    if foci_raw is None:
        return None
    s = str(foci_raw).strip()
    if not s:
        return None
    try:
        items = json.loads(s)
        if isinstance(items, str):
            items = [items]
        if not isinstance(items, list):
            return None
        return any(str(x).strip().lower() == "punctate_echogenic_foci" for x in items)
    except (json.JSONDecodeError, TypeError):
        return None


def score_kwak(row: dict) -> dict:
    """Apply Kwak 2011 scoring to one row."""
    composition_raw = _coerce(row.get("composition"))
    echogenicity = _coerce(row.get("echogenicity"))
    shape = _coerce(row.get("shape"))
    margins = _coerce(row.get("margins"))
    foci_raw = row.get("echogenic_foci")
    size_cm = row.get("size_cm_max")

    comp_norm = normalize_composition_acr(composition_raw)

    has_4core = (
        composition_raw is not None
        and echogenicity is not None
        and shape is not None
        and margins is not None
    )

    if not has_4core:
        return {
            "kwak_n_suspicious_features": None,
            "kwak_features_used_json": None,
            "kwak_category": None,
            "kwak_fna_recommended": None,
        }

    # Feature tests (microcalc treated as False when foci null — absence-of-evidence)
    f1_solid = composition_raw in ("solid", "predominantly_solid")
    f2_hypo = echogenicity in ("hypoechoic", "very_hypoechoic")
    f3_irreg = margins in ("irregular", "lobulated", "microlobulated")
    micro = _has_microcalc(foci_raw)
    f4_micro = micro is True  # null/false → False
    f5_taller = shape == "taller_than_wide"

    n_susp = sum([f1_solid, f2_hypo, f3_irreg, f4_micro, f5_taller])
    features = []
    if f1_solid:
        features.append("solid_composition")
    if f2_hypo:
        features.append("hypoechogenicity")
    if f3_irreg:
        features.append("irregular_margins")
    if f4_micro:
        features.append("microcalcifications")
    if f5_taller:
        features.append("taller_than_wide")

    # Category
    if comp_norm == "cystic" and n_susp == 0:
        category = "2"
    elif comp_norm == "spongiform" and n_susp == 0:
        category = "2"
    elif n_susp == 0:
        category = "3"
    elif n_susp == 1:
        category = "4A"
    elif n_susp == 2:
        category = "4B"
    elif n_susp in (3, 4):
        category = "4C"
    elif n_susp == 5:
        category = "5"
    else:
        category = None

    # FNA: 4A/4B/4C/5 AND size >= 1.0
    fna = None
    if category is not None:
        if category == "2" or category == "3":
            fna = False
        elif size_cm is not None:
            fna = size_cm >= 1.0
        else:
            fna = None

    return {
        "kwak_n_suspicious_features": n_susp,
        "kwak_features_used_json": json.dumps(features) if features else "[]",
        "kwak_category": category,
        "kwak_fna_recommended": fna,
    }


SNAPSHOT_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_SNAPSHOT}`
CLUSTER BY research_id AS
SELECT
  research_id, nodule_id, us_exam_id, exam_date,
  kwak_n_suspicious_features, kwak_features_used_json, kwak_category, kwak_fna_recommended,
  CURRENT_TIMESTAMP() AS snapshot_at,
  '{PIPELINE_VERSION}' AS snapshot_pipeline
FROM `{TABLE_MULTISYS}`;
"""

PULL_SQL = f"""
SELECT nodule_id, research_id, composition, echogenicity, shape, margins,
       echogenic_foci, size_cm_max
FROM `{TABLE_NODULE_V2}`
"""


def build_ctas_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id AS
SELECT
  m.* EXCEPT (
    kwak_n_suspicious_features, kwak_features_used_json,
    kwak_category, kwak_fna_recommended,
    scored_at, scoring_pipeline_version
  ),
  s.kwak_n_suspicious_features,
  s.kwak_features_used_json,
  s.kwak_category,
  s.kwak_fna_recommended,
  CURRENT_TIMESTAMP() AS scored_at,
  COALESCE(m.scoring_pipeline_version, 'phase_b_v1') AS scoring_pipeline_version
FROM `{TABLE_MULTISYS}` m
LEFT JOIN `{TABLE_STAGING}` s USING (nodule_id);
"""


AUDIT_SQL = f"""
WITH base AS (
  SELECT
    n.nodule_id,
    n.composition, n.echogenicity, n.shape, n.margins,
    m.kwak_category
  FROM `{TABLE_MULTISYS}` m
  JOIN `{TABLE_NODULE_V2}` n USING (nodule_id)
)
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(composition IS NOT NULL AND echogenicity IS NOT NULL
          AND shape IS NOT NULL AND margins IS NOT NULL) AS feasible_kwak,
  COUNTIF(kwak_category IS NOT NULL) AS scored_kwak
FROM base;
"""

DISTRIBUTION_SQL = f"""
SELECT
  kwak_category AS category, COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct
FROM `{TABLE_MULTISYS}`
WHERE kwak_category IS NOT NULL
GROUP BY 1 ORDER BY 1;
"""


def run_audit(bq) -> dict:
    audit = dict(next(iter(bq.query(AUDIT_SQL, location=LOCATION).result())))
    _log(f"  audit row: {audit}")
    feasible = audit["feasible_kwak"] or 0
    scored = audit["scored_kwak"] or 0
    success = scored / feasible if feasible else 0.0
    _log(f"  scorer-success: {success:.4f} ({scored}/{feasible})")
    if feasible and success < 0.98:
        _halt(
            f"Audit gate FAIL: Kwak scorer-success {success:.4f} < 0.98 "
            f"({scored}/{feasible})."
        )
    dist = list(bq.query(DISTRIBUTION_SQL, location=LOCATION).result())
    for r in dist:
        _log(f"  {dict(r)}")
        if r["pct"] > 0.70:
            _halt(
                f"Audit gate FAIL: Kwak category {r['category']} dominates at "
                f"{r['pct']:.1%} > 70%."
            )
    audit["scorer_success"] = success
    audit["distribution"] = [dict(r) for r in dist]
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description="Kwak 2011 scorer (Path A patch Step 3a)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    bq = bigquery.Client(project=args.project)

    if not args.dry_run:
        _log("Step 1: Snapshot existing Kwak slice")
        _run_sql(bq, SNAPSHOT_SQL, f"Snapshot -> {TABLE_SNAPSHOT}")

    _log("Step 2: Pull primitives")
    rows = list(bq.query(PULL_SQL, location=LOCATION).result())
    _log(f"  pulled {len(rows)} rows")

    _log("Step 3: Score deterministically")
    scored = []
    n_ok = 0
    for r in rows:
        rd = dict(r)
        result = score_kwak(rd)
        result["nodule_id"] = rd["nodule_id"]
        result["research_id"] = rd["research_id"]
        scored.append(result)
        if result["kwak_category"] is not None:
            n_ok += 1
    _log(f"  scored kwak: {n_ok}")

    if args.dry_run:
        _log("DRY RUN: stopping")
        return

    _log("Step 4: Load staging")
    import pandas as pd
    df = pd.DataFrame(scored)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    bq.load_table_from_dataframe(df, TABLE_STAGING, job_config=job_config,
                                  location=LOCATION).result()
    _log(f"  staging rows: {_scalar(bq, f'SELECT COUNT(*) FROM `{TABLE_STAGING}`')}")

    _log("Step 5: CTAS rebuild")
    _run_sql(bq, build_ctas_sql(), "CTAS rebuild multisystem with Kwak cols")

    _log("Step 6: Audit")
    run_audit(bq)
    _log("Done.")


if __name__ == "__main__":
    main()
