"""
Phase E patch — Step 3c — C-TIRADS 2020 scorer
================================================
Per verbatim B.4 rules in CURSOR_PROMPT_PHASE_B_DETERMINISTIC_SCORERS_20260507.md.

Score = sum of positive features (+1 each) minus comet-tail (-1 if present).

Positive features:
  1. Solid composition (composition IN ('solid','predominantly_solid'))
  2. Microcalcifications (foci array contains 'punctate_echogenic_foci')
  3. Marked hypoechogenicity (echogenicity = 'very_hypoechoic')
  4. Ill-defined/irregular margins OR ETE on US
     - margins IN ('irregular','microlobulated','lobulated','ill_defined','extrathyroidal_extension')
     - OR JSON_VALUE(ete_us_jsonb,'$.presence') IN
       ('abutment','bulging','capsule_loss','strap_muscle_invasion')
  5. Vertical orientation (shape='taller_than_wide')

Negative feature:
  - Comet-tail artifact (foci array contains 'large_comet_tail_artifacts') → -1

ctirads_score range: -1 to +5

Category mapping:
  - C-TIRADS 2 = score = -1 (benign)
  - C-TIRADS 3 = score = 0
  - C-TIRADS 4A = score = 1
  - C-TIRADS 4B = score = 2
  - C-TIRADS 4C = score IN (3, 4)
  - C-TIRADS 5 = score = 5
  - C-TIRADS 6 = NOT assigned from US (path-confirmed only; reserved)

FNA:
  - 2, 3: never FNA
  - 4A: >= 1.5 cm
  - 4B, 4C: >= 1.0 cm
  - 5: >= 1.0 cm

Audit gate: scorer-success-rate >= 98% on feasible rows where the 4 core
primitives (composition + echogenicity + shape + margins) are all non-NULL.

Author: Cursor Agent (Path A patch — Step 3c), 2026-05-08
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

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"
LOCATION = "us-central1"

TABLE_MULTISYS = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_tirads_multisystem_v1"
TABLE_NODULE_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_v2"
TABLE_SNAPSHOT = (
    f"{PROJECT}.{DATASET_WS}.cpm_pre_ctirads_surface_to_multisystem_20260508_snapshot"
)
TABLE_STAGING = f"{PROJECT}.{DATASET_WS}.tirads_ctirads_scored_v1"
PIPELINE_VERSION = "phase_e_patch_ctirads_v1_20260508"

ETE_PRESENCE_POSITIVE = {"abutment", "bulging", "capsule_loss", "strap_muscle_invasion"}


def _log(m): print(f"[{datetime.now(timezone.utc).isoformat()}] {m}", flush=True)
def _halt(r): _log(f"HALT: {r}"); sys.exit(2)
def _scalar(bq, sql): return next(iter(bq.query(sql, location=LOCATION).result()))[0]
def _run_sql(bq, sql, label):
    _log(f"SQL: {label}")
    job = bq.query(sql, location=LOCATION); job.result()
    _log(f"  ok job_id={job.job_id}")


def _coerce(v):
    if v is None: return None
    s = str(v).strip().lower()
    return s if s else None


def _parse_foci(raw) -> Optional[list[str]]:
    if raw is None: return None
    s = str(raw).strip()
    if not s: return None
    try:
        items = json.loads(s)
        if isinstance(items, str): items = [items]
        if not isinstance(items, list): return None
        return [str(x).strip().lower() for x in items if x is not None]
    except (json.JSONDecodeError, TypeError):
        return None


def _ete_presence_positive(ete_jsonb_raw) -> bool:
    """Return True if ete_us_jsonb has $.presence in the positive set."""
    if ete_jsonb_raw is None:
        return False
    s = str(ete_jsonb_raw).strip()
    if not s:
        return False
    try:
        d = json.loads(s)
        if isinstance(d, dict):
            presence = str(d.get("presence", "")).strip().lower()
            return presence in ETE_PRESENCE_POSITIVE
    except (json.JSONDecodeError, TypeError):
        return False
    return False


def score_ctirads(row: dict) -> dict:
    composition = _coerce(row.get("composition"))
    echogenicity = _coerce(row.get("echogenicity"))
    shape = _coerce(row.get("shape"))
    margins = _coerce(row.get("margins"))
    foci = _parse_foci(row.get("echogenic_foci"))
    size_cm = row.get("size_cm_max")
    ete_raw = row.get("ete_us_jsonb")

    has_4core = (composition is not None and echogenicity is not None
                 and shape is not None and margins is not None)
    if not has_4core:
        return {
            "ctirads_score": None,
            "ctirads_features_positive_json": None,
            "ctirads_comet_tail_present": None,
            "ctirads_category": None,
            "ctirads_fna_recommended": None,
        }

    # Positive feature tests
    positives: list[str] = []
    if composition in ("solid", "predominantly_solid"):
        positives.append("solid_composition")
    if foci and "punctate_echogenic_foci" in foci:
        positives.append("microcalcifications")
    if echogenicity == "very_hypoechoic":
        positives.append("marked_hypoechogenicity")

    margins_irreg_or_ete = (
        margins in ("irregular", "microlobulated", "lobulated", "ill_defined",
                    "extrathyroidal_extension")
        or _ete_presence_positive(ete_raw)
    )
    if margins_irreg_or_ete:
        positives.append("irregular_margins_or_ete")

    if shape == "taller_than_wide":
        positives.append("taller_than_wide")

    # Comet-tail negative
    comet_present = bool(foci and "large_comet_tail_artifacts" in foci)

    score = len(positives) - (1 if comet_present else 0)

    # Category mapping
    if score == -1:
        category = "2"
    elif score == 0:
        category = "3"
    elif score == 1:
        category = "4A"
    elif score == 2:
        category = "4B"
    elif score in (3, 4):
        category = "4C"
    elif score == 5:
        category = "5"
    else:
        # Should not occur (range -1..5); guard for unexpected values
        category = None

    # FNA
    fna = None
    if category in ("2", "3"):
        fna = False
    elif size_cm is not None:
        if category == "4A":
            fna = size_cm >= 1.5
        elif category in ("4B", "4C", "5"):
            fna = size_cm >= 1.0

    return {
        "ctirads_score": score,
        "ctirads_features_positive_json": json.dumps(positives) if positives else "[]",
        "ctirads_comet_tail_present": comet_present,
        "ctirads_category": category,
        "ctirads_fna_recommended": fna,
    }


SNAPSHOT_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_SNAPSHOT}`
CLUSTER BY research_id AS
SELECT
  research_id, nodule_id, us_exam_id, exam_date,
  ctirads_score, ctirads_features_positive_json, ctirads_comet_tail_present,
  ctirads_category, ctirads_fna_recommended,
  CURRENT_TIMESTAMP() AS snapshot_at,
  '{PIPELINE_VERSION}' AS snapshot_pipeline
FROM `{TABLE_MULTISYS}`;
"""

PULL_SQL = f"""
SELECT nodule_id, research_id, composition, echogenicity, shape, margins,
       echogenic_foci, size_cm_max, ete_us_jsonb
FROM `{TABLE_NODULE_V2}`
"""


def build_ctas_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id AS
SELECT
  m.* EXCEPT (
    ctirads_score, ctirads_features_positive_json, ctirads_comet_tail_present,
    ctirads_category, ctirads_fna_recommended,
    scored_at, scoring_pipeline_version
  ),
  s.ctirads_score,
  s.ctirads_features_positive_json,
  s.ctirads_comet_tail_present,
  s.ctirads_category,
  s.ctirads_fna_recommended,
  CURRENT_TIMESTAMP() AS scored_at,
  COALESCE(m.scoring_pipeline_version, 'phase_b_v1') AS scoring_pipeline_version
FROM `{TABLE_MULTISYS}` m
LEFT JOIN `{TABLE_STAGING}` s USING (nodule_id);
"""

AUDIT_SQL = f"""
WITH base AS (
  SELECT n.nodule_id, n.composition, n.echogenicity, n.shape, n.margins,
         m.ctirads_category
  FROM `{TABLE_MULTISYS}` m JOIN `{TABLE_NODULE_V2}` n USING (nodule_id)
)
SELECT COUNT(*) AS total_rows,
  COUNTIF(composition IS NOT NULL AND echogenicity IS NOT NULL
          AND shape IS NOT NULL AND margins IS NOT NULL) AS feasible_ctirads,
  COUNTIF(ctirads_category IS NOT NULL) AS scored_ctirads
FROM base;
"""

DISTRIBUTION_SQL = f"""
SELECT ctirads_category AS category, COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct
FROM `{TABLE_MULTISYS}` WHERE ctirads_category IS NOT NULL GROUP BY 1 ORDER BY 1;
"""


def run_audit(bq) -> dict:
    audit = dict(next(iter(bq.query(AUDIT_SQL, location=LOCATION).result())))
    _log(f"  audit row: {audit}")
    feasible = audit["feasible_ctirads"] or 0
    scored = audit["scored_ctirads"] or 0
    success = scored / feasible if feasible else 0.0
    _log(f"  scorer-success: {success:.4f} ({scored}/{feasible})")
    if feasible and success < 0.98:
        _halt(f"C-TIRADS scorer-success {success:.4f} < 0.98")
    dist = list(bq.query(DISTRIBUTION_SQL, location=LOCATION).result())
    for r in dist:
        _log(f"  {dict(r)}")
        if r["pct"] > 0.70:
            _halt(f"C-TIRADS category {r['category']} dominates at {r['pct']:.1%}")
    return audit


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--project", default=PROJECT)
    args = p.parse_args()
    bq = bigquery.Client(project=args.project)

    if not args.dry_run:
        _log("Step 1: Snapshot")
        _run_sql(bq, SNAPSHOT_SQL, f"Snapshot -> {TABLE_SNAPSHOT}")

    _log("Step 2: Pull primitives")
    rows = list(bq.query(PULL_SQL, location=LOCATION).result())
    _log(f"  pulled {len(rows)} rows")

    _log("Step 3: Score")
    scored = []
    n_ok = 0
    for r in rows:
        rd = dict(r)
        result = score_ctirads(rd)
        result["nodule_id"] = rd["nodule_id"]
        result["research_id"] = rd["research_id"]
        scored.append(result)
        if result["ctirads_category"] is not None:
            n_ok += 1
    _log(f"  scored ctirads: {n_ok}")

    if args.dry_run:
        _log("DRY RUN: stopping"); return

    _log("Step 4: Load staging")
    import pandas as pd
    df = pd.DataFrame(scored)
    job = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    bq.load_table_from_dataframe(df, TABLE_STAGING, job_config=job, location=LOCATION).result()
    _log(f"  staging rows: {_scalar(bq, f'SELECT COUNT(*) FROM `{TABLE_STAGING}`')}")

    _log("Step 5: CTAS rebuild")
    _run_sql(bq, build_ctas_sql(), "CTAS rebuild multisystem with C-TIRADS cols")

    _log("Step 6: Audit")
    run_audit(bq)
    _log("Done.")


if __name__ == "__main__":
    main()
