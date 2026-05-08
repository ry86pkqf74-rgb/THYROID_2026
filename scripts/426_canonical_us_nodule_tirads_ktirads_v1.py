"""
Phase E patch — Step 3b — K-TIRADS 2021 scorer
================================================
Per verbatim B.3 rules in CURSOR_PROMPT_PHASE_B_DETERMINISTIC_SCORERS_20260507.md.

Composition class (mutually exclusive, evaluated in order):
  - iso_hyperechoic_spongiform: composition='spongiform' AND echo IN (hyperechoic, isoechoic)
  - partially_cystic_intracystic_foci: composition IN (mixed_cystic_solid, predominantly_cystic)
       AND echogenic_foci array contains 'large_comet_tail_artifacts'
  - pure_cyst: composition='cystic' AND foci is ['none'] OR null/empty
  - partially_cystic_iso_hyper: composition IN (mixed_cystic_solid, predominantly_cystic)
       OR echogenicity IN (isoechoic, hyperechoic) (and not a more specific class above)
  - solid_hypoechoic: composition IN (solid, predominantly_solid)
       AND echogenicity IN (hypoechoic, very_hypoechoic)
  - else: 'other'

Three suspicious features (count for ktirads_n_suspicious):
  1. Punctate echogenic foci (foci array contains 'punctate_echogenic_foci')
  2. Nonparallel orientation (shape='taller_than_wide')
  3. Irregular margins (margins IN ('irregular','microlobulated','lobulated'))

ktirads_entirely_calcified BOOL from canonical_us_nodule_v2.entirely_calcified.

Category mapping:
  - K-TIRADS 2 = composition_class IN ('iso_hyperechoic_spongiform',
                                        'partially_cystic_intracystic_foci',
                                        'pure_cyst')
  - K-TIRADS 3 = composition_class='partially_cystic_iso_hyper' AND n_susp=0
  - K-TIRADS 4 = (composition_class='solid_hypoechoic' AND n_susp=0)
                  OR (composition_class='partially_cystic_iso_hyper' AND n_susp>=1)
                  OR entirely_calcified=TRUE
  - K-TIRADS 5 = composition_class='solid_hypoechoic' AND n_susp>=1

FNA size thresholds: 2: never; 3: > 2.0; 4: >= 1.5 (conservative);
5: > 1.0.

Audit gate: scorer-success-rate >= 98% on feasible rows (composition + echogenicity
present at minimum, since composition_class always evaluates from those two).

Author: Cursor Agent (Path A patch — Step 3b), 2026-05-08
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
    f"{PROJECT}.{DATASET_WS}.cpm_pre_ktirads_surface_to_multisystem_20260508_snapshot"
)
TABLE_STAGING = f"{PROJECT}.{DATASET_WS}.tirads_ktirads_scored_v1"
PIPELINE_VERSION = "phase_e_patch_ktirads_v1_20260508"


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


def _parse_foci(raw):
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


def derive_composition_class(composition: Optional[str], echogenicity: Optional[str],
                             foci: Optional[list]) -> Optional[str]:
    """Mutually exclusive K-TIRADS composition class."""
    c = _coerce(composition)
    e = _coerce(echogenicity)
    if not c or not e:
        return None

    # Rule 1: spongiform + iso/hyperechoic
    if c == "spongiform" and e in ("isoechoic", "hyperechoic"):
        return "iso_hyperechoic_spongiform"

    # Rule 2: partially-cystic with intracystic comet-tail
    if c in ("mixed_cystic_solid", "mixed cystic and solid", "mixed",
             "predominantly_cystic"):
        if foci and "large_comet_tail_artifacts" in foci:
            return "partially_cystic_intracystic_foci"

    # Rule 3: pure cyst
    if c in ("cystic", "anechoic"):
        if foci is None or foci == [] or foci == ["none"]:
            return "pure_cyst"

    # Rule 5: solid hypoechoic (check before rule 4 which is broader)
    if c in ("solid", "predominantly_solid") and e in ("hypoechoic", "very_hypoechoic"):
        return "solid_hypoechoic"

    # Rule 4: partially-cystic iso/hyper (broader fallback)
    if c in ("mixed_cystic_solid", "mixed cystic and solid", "mixed",
             "predominantly_cystic"):
        return "partially_cystic_iso_hyper"
    if e in ("isoechoic", "hyperechoic"):
        return "partially_cystic_iso_hyper"

    return "other"


def score_ktirads(row: dict) -> dict:
    composition = _coerce(row.get("composition"))
    echogenicity = _coerce(row.get("echogenicity"))
    shape = _coerce(row.get("shape"))
    margins = _coerce(row.get("margins"))
    foci = _parse_foci(row.get("echogenic_foci"))
    size_cm = row.get("size_cm_max")
    entirely_calc = row.get("entirely_calcified")

    if composition is None or echogenicity is None:
        return {
            "ktirads_composition_class": None,
            "ktirads_n_suspicious": None,
            "ktirads_entirely_calcified": None,
            "ktirads_category": None,
            "ktirads_fna_recommended": None,
        }

    comp_class = derive_composition_class(composition, echogenicity, foci)

    # Suspicious feature count (3 features) — these only meaningfully matter
    # for the partially_cystic_iso_hyper / solid_hypoechoic branches, but
    # compute always for the column.
    f1_microcalc = bool(foci and "punctate_echogenic_foci" in foci)
    f2_taller = shape == "taller_than_wide"
    f3_irreg = margins in ("irregular", "microlobulated", "lobulated")
    n_susp = sum([f1_microcalc, f2_taller, f3_irreg])

    ec_bool = bool(entirely_calc) if entirely_calc is not None else False

    # Category. Decision order matters:
    #   1) entirely_calcified=TRUE always upgrades to K-TIRADS 4 regardless
    #      of composition_class (clinical intent of the override clause —
    #      an entirely-calcified nodule warrants follow-up even if the
    #      background composition would otherwise read benign).
    #   2) Otherwise the composition_class drives the category per B.3.
    category = None
    if ec_bool:
        category = "4"
    elif comp_class in ("iso_hyperechoic_spongiform",
                        "partially_cystic_intracystic_foci",
                        "pure_cyst"):
        category = "2"
    elif comp_class == "solid_hypoechoic" and n_susp >= 1:
        category = "5"
    elif (
        (comp_class == "solid_hypoechoic" and n_susp == 0)
        or (comp_class == "partially_cystic_iso_hyper" and n_susp >= 1)
    ):
        category = "4"
    elif comp_class == "partially_cystic_iso_hyper" and n_susp == 0:
        category = "3"
    elif comp_class == "other":
        # Fall-through: assign 3 if no suspicious features, else 4
        category = "4" if n_susp >= 1 else "3"

    # FNA
    fna = None
    if category == "2":
        fna = False
    elif category == "3" and size_cm is not None:
        fna = size_cm > 2.0
    elif category == "4" and size_cm is not None:
        fna = size_cm >= 1.5
    elif category == "5" and size_cm is not None:
        fna = size_cm > 1.0
    elif category is not None and size_cm is None:
        fna = None

    return {
        "ktirads_composition_class": comp_class,
        "ktirads_n_suspicious": n_susp,
        "ktirads_entirely_calcified": ec_bool,
        "ktirads_category": category,
        "ktirads_fna_recommended": fna,
    }


SNAPSHOT_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_SNAPSHOT}`
CLUSTER BY research_id AS
SELECT
  research_id, nodule_id, us_exam_id, exam_date,
  ktirads_composition_class, ktirads_n_suspicious, ktirads_entirely_calcified,
  ktirads_category, ktirads_fna_recommended,
  CURRENT_TIMESTAMP() AS snapshot_at,
  '{PIPELINE_VERSION}' AS snapshot_pipeline
FROM `{TABLE_MULTISYS}`;
"""

PULL_SQL = f"""
SELECT nodule_id, research_id, composition, echogenicity, shape, margins,
       echogenic_foci, size_cm_max, entirely_calcified
FROM `{TABLE_NODULE_V2}`
"""


def build_ctas_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id AS
SELECT
  m.* EXCEPT (
    ktirads_composition_class, ktirads_n_suspicious, ktirads_entirely_calcified,
    ktirads_category, ktirads_fna_recommended,
    scored_at, scoring_pipeline_version
  ),
  s.ktirads_composition_class,
  s.ktirads_n_suspicious,
  s.ktirads_entirely_calcified,
  s.ktirads_category,
  s.ktirads_fna_recommended,
  CURRENT_TIMESTAMP() AS scored_at,
  COALESCE(m.scoring_pipeline_version, 'phase_b_v1') AS scoring_pipeline_version
FROM `{TABLE_MULTISYS}` m
LEFT JOIN `{TABLE_STAGING}` s USING (nodule_id);
"""

AUDIT_SQL = f"""
WITH base AS (
  SELECT n.nodule_id, n.composition, n.echogenicity, m.ktirads_category
  FROM `{TABLE_MULTISYS}` m JOIN `{TABLE_NODULE_V2}` n USING (nodule_id)
)
SELECT COUNT(*) AS total_rows,
  COUNTIF(composition IS NOT NULL AND echogenicity IS NOT NULL) AS feasible_ktirads,
  COUNTIF(ktirads_category IS NOT NULL) AS scored_ktirads
FROM base;
"""

DISTRIBUTION_SQL = f"""
SELECT ktirads_category AS category, COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct
FROM `{TABLE_MULTISYS}` WHERE ktirads_category IS NOT NULL GROUP BY 1 ORDER BY 1;
"""


def run_audit(bq) -> dict:
    audit = dict(next(iter(bq.query(AUDIT_SQL, location=LOCATION).result())))
    _log(f"  audit row: {audit}")
    feasible = audit["feasible_ktirads"] or 0
    scored = audit["scored_ktirads"] or 0
    success = scored / feasible if feasible else 0.0
    _log(f"  scorer-success: {success:.4f} ({scored}/{feasible})")
    if feasible and success < 0.98:
        _halt(f"K-TIRADS scorer-success {success:.4f} < 0.98")
    dist = list(bq.query(DISTRIBUTION_SQL, location=LOCATION).result())
    for r in dist:
        _log(f"  {dict(r)}")
        if r["pct"] > 0.70:
            _halt(f"K-TIRADS category {r['category']} dominates at {r['pct']:.1%}")
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
        result = score_ktirads(rd)
        result["nodule_id"] = rd["nodule_id"]
        result["research_id"] = rd["research_id"]
        scored.append(result)
        if result["ktirads_category"] is not None:
            n_ok += 1
    _log(f"  scored ktirads: {n_ok}")

    if args.dry_run:
        _log("DRY RUN: stopping"); return

    _log("Step 4: Load staging")
    import pandas as pd
    df = pd.DataFrame(scored)
    job = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    bq.load_table_from_dataframe(df, TABLE_STAGING, job_config=job, location=LOCATION).result()
    _log(f"  staging rows: {_scalar(bq, f'SELECT COUNT(*) FROM `{TABLE_STAGING}`')}")

    _log("Step 5: CTAS rebuild")
    _run_sql(bq, build_ctas_sql(), "CTAS rebuild multisystem with K-TIRADS cols")

    _log("Step 6: Audit")
    run_audit(bq)
    _log("Done.")


if __name__ == "__main__":
    main()
