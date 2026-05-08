"""
Phase E patch — Step 3d — SRU 2005 FNA-decision-tree scorer
=============================================================
Per verbatim B.5 rules in CURSOR_PROMPT_PHASE_B_DETERMINISTIC_SCORERS_20260507.md.

NOT a malignancy score. Outputs an FNA recommendation per a 5-step
ordered decision tree. First match wins:

  1. lymph_node_priority — pub_workspace.us_nodule_ln_context_v1.has_suspicious_ln_within_60d=1
  2. fna_strong — microcalcifications AND size_cm_max >= 1.0
  3. fna_consider —
     a. (solid OR coarse calcifications) AND size_cm_max >= 1.5
     b. (mixed_cystic_solid OR predominantly_cystic) AND size_cm_max >= 2.0
        (no mural-mention available; default to composition match alone)
     c. interval_growth = TRUE
  4. no_fna — composition IN ('cystic','predominantly_cystic')
              AND interval_growth IS NOT TRUE
  5. NULL — insufficient data

sru_basis_json captures which rule fired and the supporting features.

Audit gate: distribution sanity ONLY (no recommendation > 70% of scored rows).
SRU 2005 is fundamentally an FNA decision-support tree, not a universal
classifier — many composition×size combinations don't match any rule and
correctly return NULL (e.g., mixed_cystic_solid below 2cm without interval
growth or microcalc). Reporting a scorer-success-rate against an "ought-to-
score" denominator would conflate "scorer broken" with "SRU has no opinion."
Per Phase B B.5 prompt: distribution sanity is the audit norm. Cohort-level
coverage (scored rows / total) is reported in the DFL summary as informational.

Author: Cursor Agent (Path A patch — Step 3d), 2026-05-08
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
TABLE_LN_CTX = f"{PROJECT}.{DATASET_WS}.us_nodule_ln_context_v1"
TABLE_SNAPSHOT = (
    f"{PROJECT}.{DATASET_WS}.cpm_pre_sru_surface_to_multisystem_20260508_snapshot"
)
TABLE_STAGING = f"{PROJECT}.{DATASET_WS}.tirads_sru_scored_v1"
PIPELINE_VERSION = "phase_e_patch_sru_v1_20260508"


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


def score_sru(row: dict) -> dict:
    """Apply SRU 2005 ordered decision tree."""
    composition = _coerce(row.get("composition"))
    foci = _parse_foci(row.get("echogenic_foci"))
    size_cm = row.get("size_cm_max")
    interval_growth = row.get("interval_growth")
    ln_60d_int = row.get("has_suspicious_ln_within_60d")
    ln_priority = ln_60d_int == 1

    has_microcalc = bool(foci and "punctate_echogenic_foci" in foci)
    has_macrocalc = bool(foci and "macrocalcifications" in foci)
    is_solid = composition in ("solid", "predominantly_solid")
    is_mixed_or_pcystic = composition in (
        "mixed_cystic_solid", "mixed cystic and solid", "mixed",
        "predominantly_cystic",
    )
    is_pure_cystic = composition in ("cystic", "predominantly_cystic", "anechoic")

    # Apply rules in priority order
    if ln_priority:
        return {
            "sru_recommendation": "lymph_node_priority",
            "sru_basis_json": json.dumps({
                "rule": "lymph_node_priority",
                "support": {"has_suspicious_ln_within_60d": True},
            }),
        }

    if has_microcalc and size_cm is not None and size_cm >= 1.0:
        return {
            "sru_recommendation": "fna_strong",
            "sru_basis_json": json.dumps({
                "rule": "fna_strong",
                "support": {"microcalcifications": True, "size_cm_max": size_cm},
            }),
        }

    if size_cm is not None:
        # 3a: solid OR coarse calcifications, size >= 1.5
        if (is_solid or has_macrocalc) and size_cm >= 1.5:
            return {
                "sru_recommendation": "fna_consider",
                "sru_basis_json": json.dumps({
                    "rule": "fna_consider_solid_or_coarse",
                    "support": {
                        "solid": is_solid, "macrocalcifications": has_macrocalc,
                        "size_cm_max": size_cm,
                    },
                }),
            }
        # 3b: mixed/predominantly_cystic, size >= 2.0
        if is_mixed_or_pcystic and size_cm >= 2.0:
            return {
                "sru_recommendation": "fna_consider",
                "sru_basis_json": json.dumps({
                    "rule": "fna_consider_mixed_predcystic",
                    "support": {
                        "composition": composition, "size_cm_max": size_cm,
                    },
                }),
            }

    # 3c: interval_growth = TRUE
    if interval_growth is True:
        return {
            "sru_recommendation": "fna_consider",
            "sru_basis_json": json.dumps({
                "rule": "fna_consider_interval_growth",
                "support": {"interval_growth": True},
            }),
        }

    # 4: no_fna for cystic/predominantly_cystic without growth
    if is_pure_cystic and interval_growth is not True:
        return {
            "sru_recommendation": "no_fna",
            "sru_basis_json": json.dumps({
                "rule": "no_fna_cystic_no_growth",
                "support": {
                    "composition": composition,
                    "interval_growth": interval_growth,
                },
            }),
        }

    # 5: NULL - insufficient data
    return {"sru_recommendation": None, "sru_basis_json": None}


SNAPSHOT_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_SNAPSHOT}`
CLUSTER BY research_id AS
SELECT
  research_id, nodule_id, us_exam_id, exam_date,
  sru_recommendation, sru_basis_json,
  CURRENT_TIMESTAMP() AS snapshot_at,
  '{PIPELINE_VERSION}' AS snapshot_pipeline
FROM `{TABLE_MULTISYS}`;
"""

PULL_SQL = f"""
SELECT
  n.nodule_id, n.research_id,
  n.composition, n.echogenic_foci, n.size_cm_max, n.interval_growth,
  l.has_suspicious_ln_within_60d
FROM `{TABLE_NODULE_V2}` n
LEFT JOIN `{TABLE_LN_CTX}` l USING (nodule_id)
"""


def build_ctas_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id AS
SELECT
  m.* EXCEPT (
    sru_recommendation, sru_basis_json,
    scored_at, scoring_pipeline_version
  ),
  s.sru_recommendation,
  s.sru_basis_json,
  CURRENT_TIMESTAMP() AS scored_at,
  COALESCE(m.scoring_pipeline_version, 'phase_b_v1') AS scoring_pipeline_version
FROM `{TABLE_MULTISYS}` m
LEFT JOIN `{TABLE_STAGING}` s USING (nodule_id);
"""

AUDIT_SQL = f"""
WITH base AS (
  SELECT
    n.nodule_id, n.composition, n.size_cm_max,
    l.has_suspicious_ln_within_60d,
    m.sru_recommendation
  FROM `{TABLE_MULTISYS}` m
  JOIN `{TABLE_NODULE_V2}` n USING (nodule_id)
  LEFT JOIN `{TABLE_LN_CTX}` l USING (nodule_id)
)
SELECT COUNT(*) AS total_rows,
  -- Reported as cohort coverage; not used as a gate.
  COUNTIF(composition IS NOT NULL OR has_suspicious_ln_within_60d = 1)
    AS rows_with_some_input,
  COUNTIF(composition IS NOT NULL AND size_cm_max IS NOT NULL)
    AS rows_with_composition_and_size,
  COUNTIF(sru_recommendation IS NOT NULL) AS scored_sru
FROM base;
"""

DISTRIBUTION_SQL = f"""
SELECT sru_recommendation AS recommendation, COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct
FROM `{TABLE_MULTISYS}` WHERE sru_recommendation IS NOT NULL
GROUP BY 1 ORDER BY 1;
"""


def run_audit(bq) -> dict:
    audit = dict(next(iter(bq.query(AUDIT_SQL, location=LOCATION).result())))
    _log(f"  audit row: {audit}")
    rows_with_input = audit["rows_with_some_input"] or 0
    rows_comp_size = audit["rows_with_composition_and_size"] or 0
    scored = audit["scored_sru"] or 0
    # Cohort coverage is informational only — SRU 2005 returns NULL by design
    # when no rule matches (e.g., spongiform without growth).
    coverage_input = scored / rows_with_input if rows_with_input else 0.0
    coverage_cs = scored / rows_comp_size if rows_comp_size else 0.0
    _log(
        f"  cohort coverage: scored={scored}; "
        f"composition-or-LN denom={coverage_input:.4f} ({scored}/{rows_with_input}); "
        f"composition-and-size denom={coverage_cs:.4f} ({scored}/{rows_comp_size}). "
        f"NULLs are intentional per SRU rules."
    )
    # Distribution sanity (the only hard gate for SRU)
    dist = list(bq.query(DISTRIBUTION_SQL, location=LOCATION).result())
    for r in dist:
        _log(f"  {dict(r)}")
        if r["pct"] > 0.70:
            _halt(f"SRU recommendation {r['recommendation']} dominates at {r['pct']:.1%}")
    audit["coverage_composition_or_ln"] = coverage_input
    audit["coverage_composition_and_size"] = coverage_cs
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

    _log("Step 2: Pull primitives + LN context")
    rows = list(bq.query(PULL_SQL, location=LOCATION).result())
    _log(f"  pulled {len(rows)} rows")

    _log("Step 3: Score")
    scored = []
    n_ok = 0
    for r in rows:
        rd = dict(r)
        result = score_sru(rd)
        result["nodule_id"] = rd["nodule_id"]
        result["research_id"] = rd["research_id"]
        scored.append(result)
        if result["sru_recommendation"] is not None:
            n_ok += 1
    _log(f"  scored sru: {n_ok}")

    if args.dry_run:
        _log("DRY RUN: stopping"); return

    _log("Step 4: Load staging")
    import pandas as pd
    df = pd.DataFrame(scored)
    job = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    bq.load_table_from_dataframe(df, TABLE_STAGING, job_config=job, location=LOCATION).result()
    _log(f"  staging rows: {_scalar(bq, f'SELECT COUNT(*) FROM `{TABLE_STAGING}`')}")

    _log("Step 5: CTAS rebuild")
    _run_sql(bq, build_ctas_sql(), "CTAS rebuild multisystem with SRU cols")

    _log("Step 6: Audit")
    run_audit(bq)
    _log("Done.")


if __name__ == "__main__":
    main()
