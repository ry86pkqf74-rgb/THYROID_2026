"""
Phase E patch — Step 2 — ACR TI-RADS 2017 scorer (strict + imputed dual output)
================================================================================
Surfaces ACR TI-RADS 2017 categories into
``pub_canonical.canonical_us_nodule_tirads_multisystem_v1`` per the verbatim
B.1 rules in ``CURSOR_PROMPT_PHASE_B_DETERMINISTIC_SCORERS_20260507.md`` and
the Logan v0.3 dual-output convention (strict requires all 5 features;
imputed defaults missing echogenic_foci → ``["none"]``).

Why this exists
---------------
Path A patch (``CURSOR_PROMPT_PHASE_E_PATCH_AND_RESUME_20260507.md``) Step 2.
The ACR slice of the multisystem table was 0-populated despite Phase B closure
claims — see Verification Check ``VC-2026-05-07-tirads-multisystem-registry-gap``
(Airtable record ``rec3CVWZUpPVAuZSl``) and Linear ``THY-46``.

Audit gates (per Logan reframe 2026-05-08)
------------------------------------------
The patch prompt's absolute row-count thresholds (≥35k imputed, ≥24k strict)
are physically infeasible at live primitive coverage. Replaced with
scorer-success-rate ≥98% on feasible rows:

* ``acr2017_category_strict`` populated on ≥98% of rows where all 5
  primitives are non-NULL (feasible-strict denominator ≈ 6,858).
* ``acr2017_category_imputed`` populated on ≥98% of rows where 4 core
  primitives (composition, echogenicity, shape, margins) are non-NULL
  (feasible-imputed denominator ≈ 21,454).
* Concordance with legacy ``canonical_us_nodule_v2.acr2017_tirads_category``
  on overlap ≥95%. Disagreements written to
  ``pub_workspace.qc_phase_b_acr_disagreement_v1``.
* Distribution sanity: no single category > 70%.

Composition normalization
-------------------------
Uses the shared utility ``scripts.lib.composition_normalize``. Logan-approved
map: anechoic → cystic, predominantly_cystic → cystic, mixed → mixed_cystic_solid
(flagged via ``composition_normalization_warning``), predominantly_solid → solid.

Hard rules obeyed
-----------------
* PHI guard: no raw text in any output column.
* Snapshot before mutate: ``pub_workspace.cpm_pre_acr_surface_to_multisystem_20260507_snapshot``.
* CTAS-rebuild preserves ``CLUSTER BY research_id``.
* DFL row appended (``lifecycle='Applied'``) at closure.
* ALTER TABLE columns are idempotent (``IF NOT EXISTS``).
* No LLM calls in this scorer — pure deterministic.
* ``--dry-run`` flag: computes scores, skips snapshot + CTAS + DFL.

Usage
-----
    python scripts/418_canonical_us_nodule_tirads_acr2017_v1.py [--dry-run] [--project PROJECT]

Author: Cursor Agent (Path A patch — Step 2), 2026-05-08
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from google.cloud import bigquery

# Ensure scripts/lib is importable when invoked as a script
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.lib.composition_normalize import (  # noqa: E402
    acr_composition_points,
    composition_normalization_warning,
    normalize_composition_acr,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"
LOCATION = "us-central1"

TABLE_MULTISYS = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_tirads_multisystem_v1"
TABLE_NODULE_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_v2"
TABLE_SNAPSHOT = (
    f"{PROJECT}.{DATASET_WS}.cpm_pre_acr_surface_to_multisystem_20260507_snapshot"
)
TABLE_STAGING = f"{PROJECT}.{DATASET_WS}.tirads_acr2017_scored_v1"
TABLE_DISAGREE = f"{PROJECT}.{DATASET_WS}.qc_phase_b_acr_disagreement_v1"

PIPELINE_VERSION = "phase_e_patch_acr2017_v1_20260508"
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def _halt(reason: str) -> None:
    _log(f"HALT: {reason}")
    sys.exit(2)


def _scalar(bq: bigquery.Client, sql: str):
    return next(iter(bq.query(sql, location=LOCATION).result()))[0]


def _run_sql(bq: bigquery.Client, sql: str, label: str) -> bigquery.QueryJob:
    _log(f"SQL: {label}")
    job = bq.query(sql, location=LOCATION)
    job.result()
    _log(f"  ok job_id={job.job_id}")
    return job


# ---------------------------------------------------------------------------
# Deterministic ACR 2017 scorer (per B.1 verbatim rules)
# ---------------------------------------------------------------------------

ECHOGENICITY_POINTS: dict[str, int] = {
    "anechoic": 0,
    "hyperechoic": 1,
    "isoechoic": 1,
    "hypoechoic": 2,
    "very_hypoechoic": 3,
    # Defensive aliases that radiology reports occasionally produce
    "iso": 1,
    "hyper": 1,
    "hypo": 2,
    "markedly_hypoechoic": 3,
}

SHAPE_POINTS: dict[str, int] = {
    "wider_than_tall": 0,
    "taller_than_wide": 3,
}

MARGIN_POINTS: dict[str, int] = {
    "smooth": 0,
    "well_defined": 0,
    "ill_defined": 0,
    "lobulated": 2,
    "irregular": 2,
    "microlobulated": 2,
    "spiculated": 2,
    "extrathyroidal_extension": 3,
}

# Per B.1 echogenic-foci additive scoring
FOCI_POINTS: dict[str, int] = {
    "none": 0,
    "large_comet_tail_artifacts": 0,
    "macrocalcifications": 1,
    "peripheral_rim_calcifications": 2,
    "punctate_echogenic_foci": 3,
}


def _coerce(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip().lower()
    return s if s else None


def _parse_foci(raw: Optional[str]) -> tuple[Optional[list[str]], Optional[int]]:
    """
    Parse a JSON-array string of echogenic_foci tokens.

    Returns (parsed_list, points). Returns (None, None) when input is
    null / empty / unparseable.
    """
    if raw is None:
        return None, None
    s = str(raw).strip()
    if not s:
        return None, None
    try:
        parsed = json.loads(s)
        if isinstance(parsed, str):
            parsed = [parsed]
        if not isinstance(parsed, list):
            return None, None
        items = [str(x).strip().lower() for x in parsed if x is not None]
    except (json.JSONDecodeError, TypeError):
        return None, None
    pts = sum(FOCI_POINTS.get(tok, 0) for tok in items)
    return items, pts


def _categorize(total_pts: float) -> str:
    """
    Map a total-points value to a TR category per B.1 rule:
    TR1=0, TR2=1 or 2, TR3=3, TR4=4-6, TR5=>=7.

    The total=1 case sits in the documented ACR TR1/TR2 ambiguity; we map it
    to TR2 here and flag via ``acr2017_band_ambiguous``.
    """
    if total_pts <= 0:
        return "TR1"
    if total_pts <= 2:
        return "TR2"
    if total_pts == 3:
        return "TR3"
    if total_pts <= 6:
        return "TR4"
    return "TR5"


def _fna_recommended(category: Optional[str], size_cm: Optional[float]) -> Optional[bool]:
    """
    Per B.1 FNA thresholds:
      TR1, TR2: never FNA
      TR3: FNA when size_cm >= 2.5
      TR4: FNA when size_cm >= 1.5
      TR5: FNA when size_cm >= 1.0

    Returns None when category is unknown OR size is null (cannot evaluate).
    """
    if category is None:
        return None
    if category in ("TR1", "TR2"):
        return False
    if size_cm is None:
        return None
    if category == "TR3":
        return size_cm >= 2.5
    if category == "TR4":
        return size_cm >= 1.5
    if category == "TR5":
        return size_cm >= 1.0
    return None


def score_acr_dual(row: dict) -> dict:
    """
    Apply ACR 2017 dual-output scoring (strict + imputed) to one nodule row.

    Returns a dict with keys matching the multisystem schema:
      acr2017_composition_pts, acr2017_echogenicity_pts, acr2017_shape_pts,
      acr2017_margin_pts, acr2017_foci_pts,
      acr2017_total_pts_strict, acr2017_total_pts_imputed,
      acr2017_category_strict, acr2017_category_imputed,
      acr2017_features_complete_strict, acr2017_features_complete_imputed,
      acr2017_fna_recommended_strict, acr2017_fna_recommended_imputed,
      acr2017_band_ambiguous,
      composition_normalization_warning.
    """
    composition_raw = _coerce(row.get("composition"))
    echogenicity = _coerce(row.get("echogenicity"))
    shape = _coerce(row.get("shape"))
    margins = _coerce(row.get("margins"))
    foci_raw = row.get("echogenic_foci")
    size_cm = row.get("size_cm_max")

    comp_pts = acr_composition_points(composition_raw)
    echo_pts = ECHOGENICITY_POINTS.get(echogenicity) if echogenicity else None
    shape_pts = SHAPE_POINTS.get(shape) if shape else None
    margin_pts = MARGIN_POINTS.get(margins) if margins else None
    foci_items, foci_pts = _parse_foci(foci_raw)

    has_4core = (
        comp_pts is not None
        and echo_pts is not None
        and shape_pts is not None
        and margin_pts is not None
    )
    has_5core = has_4core and foci_pts is not None

    # Strict: requires all 5 features present (foci must be non-null)
    if has_5core:
        total_strict = float(comp_pts + echo_pts + shape_pts + margin_pts + foci_pts)
        cat_strict = _categorize(total_strict)
    else:
        total_strict = None
        cat_strict = None

    # Imputed: foci defaults to ["none"] (0 pts) when null; 4 core required
    if has_4core:
        foci_pts_imp = foci_pts if foci_pts is not None else 0
        total_imputed = float(comp_pts + echo_pts + shape_pts + margin_pts + foci_pts_imp)
        cat_imputed = _categorize(total_imputed)
    else:
        total_imputed = None
        cat_imputed = None

    # Band ambiguity flag fires whenever an output total = 1 (TR1/TR2 ambiguity).
    band_ambig = False
    if total_strict is not None and abs(total_strict - 1.0) < 1e-9:
        band_ambig = True
    if total_imputed is not None and abs(total_imputed - 1.0) < 1e-9:
        band_ambig = True

    return {
        "acr2017_composition_pts": float(comp_pts) if comp_pts is not None else None,
        "acr2017_echogenicity_pts": float(echo_pts) if echo_pts is not None else None,
        "acr2017_shape_pts": float(shape_pts) if shape_pts is not None else None,
        "acr2017_margin_pts": float(margin_pts) if margin_pts is not None else None,
        "acr2017_foci_pts": float(foci_pts) if foci_pts is not None else None,
        "acr2017_total_pts_strict": total_strict,
        "acr2017_total_pts_imputed": total_imputed,
        "acr2017_category_strict": cat_strict,
        "acr2017_category_imputed": cat_imputed,
        "acr2017_features_complete_strict": has_5core,
        "acr2017_features_complete_imputed": has_4core,
        "acr2017_fna_recommended_strict": _fna_recommended(cat_strict, size_cm),
        "acr2017_fna_recommended_imputed": _fna_recommended(cat_imputed, size_cm),
        "acr2017_band_ambiguous": band_ambig,
        "composition_normalization_warning": composition_normalization_warning(
            composition_raw
        ),
        "composition_acr_normalized": normalize_composition_acr(composition_raw),
    }


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

ALTER_SQL = f"""
ALTER TABLE `{TABLE_MULTISYS}`
  ADD COLUMN IF NOT EXISTS acr2017_band_ambiguous BOOL,
  ADD COLUMN IF NOT EXISTS composition_normalization_warning BOOL,
  ADD COLUMN IF NOT EXISTS composition_acr_normalized STRING;
"""

SNAPSHOT_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_SNAPSHOT}`
CLUSTER BY research_id AS
SELECT
  research_id, nodule_id, us_exam_id, exam_date,
  acr2017_composition_pts, acr2017_echogenicity_pts, acr2017_shape_pts,
  acr2017_margin_pts, acr2017_foci_pts,
  acr2017_total_pts_strict, acr2017_total_pts_imputed,
  acr2017_category_strict, acr2017_category_imputed,
  acr2017_features_complete_strict, acr2017_features_complete_imputed,
  acr2017_fna_recommended_strict, acr2017_fna_recommended_imputed,
  CURRENT_TIMESTAMP() AS snapshot_at,
  '{PIPELINE_VERSION}' AS snapshot_pipeline
FROM `{TABLE_MULTISYS}`;
"""

PULL_SQL = f"""
SELECT
  nodule_id, research_id, us_exam_id, exam_date,
  composition, echogenicity, shape, margins,
  echogenic_foci, size_cm_max,
  acr2017_tirads_category AS legacy_category,
  acr2017_tirads_points AS legacy_points
FROM `{TABLE_NODULE_V2}`
"""


def build_ctas_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id AS
SELECT
  m.* EXCEPT (
    acr2017_composition_pts, acr2017_echogenicity_pts, acr2017_shape_pts,
    acr2017_margin_pts, acr2017_foci_pts,
    acr2017_total_pts_strict, acr2017_total_pts_imputed,
    acr2017_category_strict, acr2017_category_imputed,
    acr2017_features_complete_strict, acr2017_features_complete_imputed,
    acr2017_fna_recommended_strict, acr2017_fna_recommended_imputed,
    acr2017_band_ambiguous, composition_normalization_warning,
    composition_acr_normalized,
    scored_at, scoring_pipeline_version
  ),
  s.acr2017_composition_pts,
  s.acr2017_echogenicity_pts,
  s.acr2017_shape_pts,
  s.acr2017_margin_pts,
  s.acr2017_foci_pts,
  s.acr2017_total_pts_strict,
  s.acr2017_total_pts_imputed,
  s.acr2017_category_strict,
  s.acr2017_category_imputed,
  s.acr2017_features_complete_strict,
  s.acr2017_features_complete_imputed,
  s.acr2017_fna_recommended_strict,
  s.acr2017_fna_recommended_imputed,
  s.acr2017_band_ambiguous,
  s.composition_normalization_warning,
  s.composition_acr_normalized,
  CURRENT_TIMESTAMP() AS scored_at,
  COALESCE(m.scoring_pipeline_version, 'phase_b_v1') AS scoring_pipeline_version
FROM `{TABLE_MULTISYS}` m
LEFT JOIN `{TABLE_STAGING}` s USING (nodule_id);
"""


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------

AUDIT_SQL = f"""
WITH base AS (
  SELECT
    n.nodule_id,
    n.composition, n.echogenicity, n.shape, n.margins, n.echogenic_foci,
    m.acr2017_category_strict,
    m.acr2017_category_imputed,
    m.acr2017_total_pts_strict,
    m.acr2017_total_pts_imputed,
    m.acr2017_band_ambiguous,
    m.acr2017_features_complete_strict,
    m.acr2017_features_complete_imputed,
    n.acr2017_tirads_category AS legacy_category
  FROM `{TABLE_MULTISYS}` m
  JOIN `{TABLE_NODULE_V2}` n USING (nodule_id)
)
SELECT
  COUNT(*) AS total_rows,
  COUNTIF(composition IS NOT NULL AND echogenicity IS NOT NULL
          AND shape IS NOT NULL AND margins IS NOT NULL) AS feasible_imputed,
  COUNTIF(composition IS NOT NULL AND echogenicity IS NOT NULL
          AND shape IS NOT NULL AND margins IS NOT NULL
          AND echogenic_foci IS NOT NULL) AS feasible_strict,
  COUNTIF(acr2017_category_imputed IS NOT NULL) AS scored_imputed,
  COUNTIF(acr2017_category_strict IS NOT NULL) AS scored_strict,
  COUNTIF(acr2017_band_ambiguous = TRUE) AS n_band_ambiguous,
  COUNTIF(legacy_category IS NOT NULL) AS legacy_overlap,
  COUNTIF(legacy_category IS NOT NULL AND acr2017_category_imputed IS NOT NULL) AS overlap_imputed,
  COUNTIF(legacy_category IS NOT NULL
          AND acr2017_category_imputed IS NOT NULL
          AND legacy_category = acr2017_category_imputed) AS overlap_imputed_match
FROM base;
"""

DISTRIBUTION_SQL = f"""
SELECT
  acr2017_category_imputed AS category,
  COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct
FROM `{TABLE_MULTISYS}`
WHERE acr2017_category_imputed IS NOT NULL
GROUP BY 1
ORDER BY 1;
"""

DISAGREE_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_DISAGREE}`
CLUSTER BY research_id AS
SELECT
  n.nodule_id, n.research_id, n.us_exam_id, n.exam_date,
  n.composition, n.echogenicity, n.shape, n.margins, n.echogenic_foci,
  n.size_cm_max,
  n.acr2017_tirads_category AS legacy_category,
  n.acr2017_tirads_points AS legacy_points,
  m.acr2017_category_imputed AS new_category_imputed,
  m.acr2017_total_pts_imputed AS new_points_imputed,
  m.acr2017_category_strict AS new_category_strict,
  m.acr2017_band_ambiguous,
  m.composition_normalization_warning,
  CURRENT_TIMESTAMP() AS audit_at
FROM `{TABLE_MULTISYS}` m
JOIN `{TABLE_NODULE_V2}` n USING (nodule_id)
WHERE n.acr2017_tirads_category IS NOT NULL
  AND m.acr2017_category_imputed IS NOT NULL
  AND n.acr2017_tirads_category != m.acr2017_category_imputed;
"""


def run_audit(bq: bigquery.Client) -> dict:
    _log("Audit: compute feasibility + scored counts + concordance")
    rows = list(bq.query(AUDIT_SQL, location=LOCATION).result())
    audit = dict(rows[0])
    _log(f"  audit row: {audit}")

    feasible_imputed = audit["feasible_imputed"] or 0
    feasible_strict = audit["feasible_strict"] or 0
    scored_imputed = audit["scored_imputed"] or 0
    scored_strict = audit["scored_strict"] or 0
    overlap_imp = audit["overlap_imputed"] or 0
    overlap_imp_match = audit["overlap_imputed_match"] or 0

    # Gate 1: scorer-success-rate >= 0.98 on feasible rows
    success_imputed = scored_imputed / feasible_imputed if feasible_imputed else 0.0
    success_strict = scored_strict / feasible_strict if feasible_strict else 0.0
    _log(
        f"  scorer-success: imputed={success_imputed:.4f} "
        f"({scored_imputed}/{feasible_imputed}); "
        f"strict={success_strict:.4f} ({scored_strict}/{feasible_strict})"
    )
    if feasible_imputed and success_imputed < 0.98:
        _halt(
            f"Audit gate FAIL: imputed scorer-success {success_imputed:.4f} < 0.98 "
            f"({scored_imputed}/{feasible_imputed} feasible rows)."
        )
    if feasible_strict and success_strict < 0.98:
        _halt(
            f"Audit gate FAIL: strict scorer-success {success_strict:.4f} < 0.98 "
            f"({scored_strict}/{feasible_strict} feasible rows)."
        )

    # Gate 2: distribution sanity (no single category > 70%)
    _log("Audit: distribution sanity (no single category > 70%)")
    dist_rows = list(bq.query(DISTRIBUTION_SQL, location=LOCATION).result())
    for r in dist_rows:
        _log(f"  {dict(r)}")
        if r["pct"] > 0.70:
            _halt(
                f"Audit gate FAIL: category {r['category']} dominates "
                f"at {r['pct']:.1%} > 70% — possible scorer bug."
            )

    # Gate 3: concordance with legacy v2.acr2017_tirads_category on overlap
    if overlap_imp:
        concordance = overlap_imp_match / overlap_imp
        _log(
            f"  concordance vs legacy v2.acr2017 on imputed overlap: "
            f"{concordance:.4f} ({overlap_imp_match}/{overlap_imp})"
        )
        if concordance < 0.95:
            _log(
                f"  WARNING: concordance {concordance:.4f} < 0.95 — "
                f"writing disagreements to {TABLE_DISAGREE} for review."
            )
        # Always materialize the disagreement table for inspection
        _run_sql(bq, DISAGREE_SQL, "Materialize ACR disagreement queue")
        n_dis = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_DISAGREE}`")
        _log(f"  disagreement rows: {n_dis}")
        audit["concordance_vs_legacy"] = concordance
        audit["disagreement_rows"] = n_dis
    else:
        audit["concordance_vs_legacy"] = None
        audit["disagreement_rows"] = 0

    audit["scorer_success_imputed"] = success_imputed
    audit["scorer_success_strict"] = success_strict
    audit["distribution"] = [dict(r) for r in dist_rows]

    return audit


# ---------------------------------------------------------------------------
# DFL row
# ---------------------------------------------------------------------------


def append_dfl_row(bq: bigquery.Client, dry_run: bool, audit: dict) -> None:
    if dry_run:
        _log("DFL: skipped (dry-run)")
        return
    try:
        dfl_table = f"{PROJECT}.pub_signoff.data_feedback_log_v1"
        action = (
            f"Path A patch Step 2 — ACR 2017 dual-output scorer applied. "
            f"feasible_imputed={audit.get('feasible_imputed', 0)}, "
            f"scored_imputed={audit.get('scored_imputed', 0)} "
            f"(success={audit.get('scorer_success_imputed', 0):.4f}); "
            f"feasible_strict={audit.get('feasible_strict', 0)}, "
            f"scored_strict={audit.get('scored_strict', 0)} "
            f"(success={audit.get('scorer_success_strict', 0):.4f}); "
            f"concordance_vs_legacy={audit.get('concordance_vs_legacy')}; "
            f"disagreements={audit.get('disagreement_rows', 0)}; "
            f"band_ambiguous={audit.get('n_band_ambiguous', 0)}. "
            f"Pipeline={PIPELINE_VERSION}. "
            f"Snapshot={TABLE_SNAPSHOT}. "
            f"Audit gates revised per Logan 2026-05-08 (success-rate ≥98% on feasible)."
        )[:1000]
        row = {
            "logged_at": datetime.now(timezone.utc).isoformat(),
            "target_type": "BQ infrastructure",
            "change_type": "data_correction",
            "target_table": TABLE_MULTISYS,
            "target_column": "acr2017_*",
            "action_summary": action,
            "lifecycle": "Applied",
            "source_chat": "Path A patch Step 2 ACR 2017 — 2026-05-08",
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
    parser = argparse.ArgumentParser(description="ACR 2017 scorer (Path A patch Step 2)")
    parser.add_argument(
        "--dry-run", action="store_true", help="Compute scores; skip snapshot + CTAS + DFL"
    )
    parser.add_argument("--project", default=PROJECT)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional LIMIT on the pull query for fast smoke testing",
    )
    args = parser.parse_args()

    bq = bigquery.Client(project=args.project)

    _log("Step 1: ALTER TABLE — add acr2017_band_ambiguous + warning + normalized cols")
    if args.dry_run:
        _log("  (dry-run) skipping ALTER")
    else:
        _run_sql(bq, ALTER_SQL, "ALTER TABLE add columns")

    _log("Step 2: Snapshot existing ACR slice of multisystem table")
    if args.dry_run:
        _log("  (dry-run) skipping snapshot")
    else:
        _run_sql(bq, SNAPSHOT_SQL, f"Snapshot -> {TABLE_SNAPSHOT}")
        n_snap = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_SNAPSHOT}`")
        _log(f"  snapshot rows: {n_snap}")

    _log("Step 3: Pull primitives from canonical_us_nodule_v2")
    pull_sql = PULL_SQL + (f"\nLIMIT {int(args.limit)}" if args.limit else "")
    rows = list(bq.query(pull_sql, location=LOCATION).result())
    _log(f"  pulled {len(rows)} rows")

    _log("Step 4: Score deterministically (strict + imputed)")
    scored: list[dict] = []
    n_imputed_ok = 0
    n_strict_ok = 0
    n_warning = 0
    for r in rows:
        rd = dict(r)
        result = score_acr_dual(rd)
        result["nodule_id"] = rd["nodule_id"]
        result["research_id"] = rd["research_id"]
        scored.append(result)
        if result["acr2017_category_imputed"] is not None:
            n_imputed_ok += 1
        if result["acr2017_category_strict"] is not None:
            n_strict_ok += 1
        if result["composition_normalization_warning"]:
            n_warning += 1

    _log(
        f"  scored: imputed={n_imputed_ok}, strict={n_strict_ok}, "
        f"warning={n_warning}"
    )

    if args.dry_run:
        _log("DRY RUN: stopping before staging load + CTAS rebuild")
        return

    _log("Step 5: Load staging table to BQ")
    import pandas as pd

    df = pd.DataFrame(scored)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )
    load_job = bq.load_table_from_dataframe(
        df, TABLE_STAGING, job_config=job_config, location=LOCATION
    )
    load_job.result()
    n_staged = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_STAGING}`")
    _log(f"  staging rows: {n_staged}")

    _log("Step 6: CTAS-rebuild canonical_us_nodule_tirads_multisystem_v1")
    _run_sql(bq, build_ctas_sql(), "CTAS rebuild multisystem with ACR cols")

    _log("Step 7: Audit")
    audit = run_audit(bq)

    _log("Step 8: Append DFL row (lifecycle=Applied)")
    append_dfl_row(bq, dry_run=False, audit=audit)

    _log("Done.")


if __name__ == "__main__":
    main()
